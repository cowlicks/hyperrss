//! A single peer holding a feed and its keyed blobs, backed by [`Corestore`].
//!
//! Unlike the JS implementation (which has separate `Writer`/`Reader` classes), this ports both
//! into one [`Peer`]: it's a writer when the underlying Hypercore's key pair carries a secret
//! key, and a read-only reader otherwise.
//!
//! A full peer is 4 named cores (`keys`, `feed`, `blobKeys`, `blobs` in JS). The `keys` core is
//! bootstrap/discovery indirection: a [`Peer::writer`] writes its other 3 cores' public keys into
//! it once; a [`Peer::reader`] only needs to know the `keys` core's public key up front, and
//! learns the rest by replicating and reading it — mirroring JS `writer.js`'s bootstrap append and
//! `reader.js`'s `keysCore.get(0)`.
//!
//! All 4 cores replicate over one physical connection, multiplexed via
//! [`Corestore::replicate`] — a [`Peer::reader`] can open the `feed`/`blobKeys`/`blobs` cores
//! *after* the connection is already running (once it learns their keys from the `keys` core),
//! and they still attach to that same connection automatically.

use std::time::Duration;

use corestore::Corestore;
use hyperbee::Hyperbee;
use hypercore::{Hypercore, PartialKeypair, VerifyingKey};
use hypercore_handshake::CipherTrait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{feed::FeedError, KeyedBlobs, KeyedBlobsError, OrderedHyperbee};

/// Errors produced by [`Peer`].
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum PeerError {
    /// An error from [`Corestore`].
    #[error("error from corestore: {0}")]
    Corestore(#[from] corestore::Error),
    /// An error from the feed.
    #[error("error from feed: {0}")]
    Feed(#[from] FeedError),
    /// An error from the underlying [`Hyperbee`].
    #[error("error from hyperbee: {0}")]
    Hyperbee(#[from] hyperbee::HyperbeeError),
    /// An error from the keyed blobs store.
    #[error("error from keyed blobs: {0}")]
    KeyedBlobs(#[from] KeyedBlobsError),
    /// An error from the `keys` core itself.
    #[error("error from the keys core: {0}")]
    Hypercore(#[from] hypercore::HypercoreError),
    /// The `keys` core's bootstrap entry failed to decode as JSON.
    #[error("error decoding the keys entry: {0}")]
    KeysEntryDecode(#[from] serde_json::Error),
    /// A public key in the `keys` entry wasn't validly `base64url`-encoded.
    #[error("error decoding a public key: {0}")]
    KeyEncoding(#[from] data_encoding::DecodeError),
    /// A public key in the `keys` entry didn't decode to 32 bytes.
    #[error("a public key in the keys entry was not 32 bytes")]
    InvalidPublicKeyLength,
    /// A public key in the `keys` entry was not a valid ed25519 point.
    #[error("invalid public key: {0}")]
    InvalidPublicKey(#[from] signature::Error),
}

/// The public keys of a [`Peer`]'s `feed`, `blobKeys`, and `blobs` cores — the payload of the
/// `keys` core's bootstrap entry (JS: `writer.js`'s `keys.append({ keys: {...} })`).
#[derive(Debug, Clone, Copy)]
pub struct PeerKeys {
    /// The feed core's public key.
    pub feed: VerifyingKey,
    /// The `blobKeys` core's public key.
    pub blob_keys: VerifyingKey,
    /// The `blobs` core's public key.
    pub blobs: VerifyingKey,
}

/// On-the-wire (JSON) shape of [`PeerKeys`], matching JS's `encodedStrFromBuffer`/
/// `bufferFromEncodedStr` (`base64url`-encoded public keys).
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawPeerKeys {
    feed: String,
    blob_keys: String,
    blobs: String,
}

/// The `keys` core's bootstrap entry: `{ "keys": { "feed": ..., "blobKeys": ..., "blobs": ... } }`.
#[derive(Debug, Serialize, Deserialize)]
struct KeysEntry {
    keys: RawPeerKeys,
}

fn encode_key(key: &VerifyingKey) -> String {
    data_encoding::BASE64URL_NOPAD.encode(key.as_bytes())
}

fn decode_key(s: &str) -> Result<VerifyingKey, PeerError> {
    let bytes = data_encoding::BASE64URL_NOPAD.decode(s.as_bytes())?;
    let bytes: [u8; 32] = bytes.try_into().map_err(|_| PeerError::InvalidPublicKeyLength)?;
    Ok(VerifyingKey::from_bytes(&bytes)?)
}

impl From<&PeerKeys> for RawPeerKeys {
    fn from(keys: &PeerKeys) -> Self {
        Self {
            feed: encode_key(&keys.feed),
            blob_keys: encode_key(&keys.blob_keys),
            blobs: encode_key(&keys.blobs),
        }
    }
}

impl TryFrom<RawPeerKeys> for PeerKeys {
    type Error = PeerError;

    fn try_from(raw: RawPeerKeys) -> Result<Self, PeerError> {
        Ok(Self {
            feed: decode_key(&raw.feed)?,
            blob_keys: decode_key(&raw.blob_keys)?,
            blobs: decode_key(&raw.blobs)?,
        })
    }
}

/// A peer for a single hyper-rss feed and its keyed blobs. Acts as a writer when it holds the
/// feed core's secret key, and a reader otherwise. See the [module docs](self) for why this
/// replaces JS's separate `Writer`/`Reader` types.
#[derive(Debug)]
pub struct Peer {
    store: Corestore,
    keys: PeerKeys,
    keys_core: Hypercore,
    is_writer: bool,
    feed: OrderedHyperbee,
    keyed_blobs: KeyedBlobs,
}

impl Peer {
    /// Create a writer: creates (or opens) `{name}-keys`, `name` (feed), `{name}-blobKeys`, and
    /// `{name}-blobs` as locally-owned, writable named cores in `store`. If the `keys` core is
    /// still empty, writes the other 3 cores' public keys into it (JS: `writer.js`'s bootstrap
    /// append, gated on `keys.length === 0`).
    pub async fn writer(store: &Corestore, name: &str) -> Result<Self, PeerError> {
        let feed_hc = store.get_from_name(name).await?;
        let blob_keys_hc = store.get_from_name(&format!("{name}-blobKeys")).await?;
        let blobs_hc = store.get_from_name(&format!("{name}-blobs")).await?;
        let keys_core = store.get_from_name(&format!("{name}-keys")).await?;

        let keys = PeerKeys {
            feed: feed_hc.key_pair().public,
            blob_keys: blob_keys_hc.key_pair().public,
            blobs: blobs_hc.key_pair().public,
        };
        if keys_core.info().length == 0 {
            let entry = KeysEntry {
                keys: RawPeerKeys::from(&keys),
            };
            let bytes = serde_json::to_vec(&entry).expect("KeysEntry always serializes");
            keys_core.append(&bytes).await?;
        }

        Self::from_hypercores(store.clone(), feed_hc, blob_keys_hc, blobs_hc, keys_core, keys)
    }

    /// Bootstrap a reader from just the `keys` core's public key: opens that core, starts
    /// multiplexed replication over `stream` (see [`Peer::add_stream`]), then waits for the
    /// writer's bootstrap entry to learn the other 3 cores' public keys (JS: `reader.js`'s
    /// `keysCore.get(0)`). The `feed`/`blobKeys`/`blobs` cores, opened only *after* this point,
    /// attach to this same already-running connection automatically — no extra streams needed.
    ///
    /// This waits with no internal timeout, matching JS's plain `await`; wrap the call in
    /// [`tokio::time::timeout`] if you want one.
    pub async fn reader(
        store: &Corestore,
        keys_core_public_key: VerifyingKey,
        stream: impl CipherTrait + 'static,
    ) -> Result<Self, PeerError> {
        let keys_core = store.get_from_verifying_key(&keys_core_public_key).await?;
        // TODO(spawn): tracked in ../../TODO.md alongside the other `add_stream` spawn sites.
        tokio::spawn(store.replicate(stream));

        let keys = loop {
            if let Some(bytes) = keys_core.get(0).await? {
                let entry: KeysEntry = serde_json::from_slice(&bytes)?;
                break PeerKeys::try_from(entry.keys)?;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        };

        let feed_hc = store.get_from_verifying_key(&keys.feed).await?;
        let blob_keys_hc = store.get_from_verifying_key(&keys.blob_keys).await?;
        let blobs_hc = store.get_from_verifying_key(&keys.blobs).await?;

        Self::from_hypercores(store.clone(), feed_hc, blob_keys_hc, blobs_hc, keys_core, keys)
    }

    fn from_hypercores(
        store: Corestore,
        feed_hc: Hypercore,
        blob_keys_hc: Hypercore,
        blobs_hc: Hypercore,
        keys_core: Hypercore,
        keys: PeerKeys,
    ) -> Result<Self, PeerError> {
        let feed_key_pair: PartialKeypair = feed_hc.key_pair();
        let is_writer = feed_key_pair.secret.is_some();
        let feed = OrderedHyperbee::new(Hyperbee::from_hypercore(feed_hc)?);
        let keyed_blobs = KeyedBlobs::from_hypercores(blob_keys_hc, blobs_hc)?;
        Ok(Self {
            store,
            keys,
            keys_core,
            is_writer,
            feed,
            keyed_blobs,
        })
    }

    /// Whether this peer can append to the feed (it holds the feed core's secret key).
    pub fn is_writer(&self) -> bool {
        self.is_writer
    }

    /// The feed core's public key.
    pub fn public_key(&self) -> VerifyingKey {
        self.keys.feed
    }

    /// The public keys of this peer's `feed`, `blobKeys`, and `blobs` cores.
    pub fn public_keys(&self) -> PeerKeys {
        self.keys
    }

    /// This peer's `keys` core public key — the identifier to share with a [`Peer::reader`] so
    /// it can discover the other 3 cores' keys.
    pub fn keys_core_public_key(&self) -> VerifyingKey {
        self.keys_core.key_pair().public
    }

    /// Add a replication connection for this peer: every core currently open (and any opened
    /// later) is multiplexed over `stream` via [`Corestore::replicate`]. Call this once per
    /// physical connection (e.g. once per incoming swarm connection).
    pub async fn add_stream(&self, stream: impl CipherTrait + 'static) -> Result<(), PeerError> {
        // TODO(spawn): tracked in ../../TODO.md alongside the other `add_stream` spawn sites.
        tokio::spawn(self.store.replicate(stream));
        Ok(())
    }

    /// The peer's feed.
    pub fn feed(&self) -> &OrderedHyperbee {
        &self.feed
    }

    /// The peer's keyed blobs store.
    pub fn keyed_blobs(&self) -> &KeyedBlobs {
        &self.keyed_blobs
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use hypercore_handshake::{
        state_machine::{hc_specific::generate_keypair, SecStream},
        Cipher,
    };
    use tokio_util::compat::TokioAsyncReadCompatExt;
    use uint24le_framing::Uint24LELengthPrefixedFraming;

    /// Create a pair of connected in-memory encrypted streams, mirroring
    /// `bee/src/test/replicate.rs`'s helper of the same name.
    fn create_connected_streams() -> (impl CipherTrait + 'static, impl CipherTrait + 'static) {
        let (a_b, b_a) = tokio::io::duplex(64 * 1024);
        let a_b = Uint24LELengthPrefixedFraming::new(a_b.compat());
        let b_a = Uint24LELengthPrefixedFraming::new(b_a.compat());
        let keypair = generate_keypair().unwrap();
        let initiator = Cipher::new(
            Some(Box::new(a_b)),
            SecStream::new_initiator_xx(&[]).unwrap().into(),
        );
        let responder = Cipher::new(
            Some(Box::new(b_a)),
            SecStream::new_responder_xx(&keypair, &[]).unwrap().into(),
        );
        (initiator, responder)
    }

    /// Connect a fresh writer and reader `Peer` over one multiplexed connection, carrying all
    /// 4 cores (`keys`, `feed`, `blobKeys`, `blobs`).
    async fn connected_writer_and_reader() -> Result<(Peer, Peer), Box<dyn std::error::Error>> {
        let store_a = Corestore::new_mem().await;
        let store_b = Corestore::new_mem().await;
        let writer = Peer::writer(&store_a, "feed").await?;

        let (a_to_b, b_to_a) = create_connected_streams();
        writer.add_stream(a_to_b).await?;
        let reader = Peer::reader(&store_b, writer.keys_core_public_key(), b_to_a).await?;

        Ok((writer, reader))
    }

    #[tokio::test]
    async fn writer_is_writer_reader_is_not() -> Result<(), Box<dyn std::error::Error>> {
        let (writer, reader) = connected_writer_and_reader().await?;
        assert!(writer.is_writer());
        assert!(!reader.is_writer());
        Ok(())
    }

    #[tokio::test]
    async fn reader_discovers_the_writers_keys() -> Result<(), Box<dyn std::error::Error>> {
        let (writer, reader) = connected_writer_and_reader().await?;
        assert_eq!(reader.public_key(), writer.public_key());
        let (w, r) = (writer.public_keys(), reader.public_keys());
        assert_eq!(w.blob_keys, r.blob_keys);
        assert_eq!(w.blobs, r.blobs);
        Ok(())
    }

    #[tokio::test]
    async fn writer_and_reader_replicate_a_feed() -> Result<(), Box<dyn std::error::Error>> {
        let (writer, reader) = connected_writer_and_reader().await?;

        writer.feed().put_ordered_item(b"hello", b"world").await?;

        let mut n_loops = 0;
        loop {
            if let Ok(true) = reader.feed().has_key(b"hello").await {
                break;
            }
            if n_loops > 25 {
                panic!("reader never saw the writer's item");
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
            n_loops += 1;
        }
        Ok(())
    }

    #[tokio::test]
    async fn writer_and_reader_replicate_keyed_blobs() -> Result<(), Box<dyn std::error::Error>> {
        let (writer, reader) = connected_writer_and_reader().await?;

        writer
            .keyed_blobs()
            .put(b"picture.png", b"not really a png")
            .await?;

        let mut n_loops = 0;
        loop {
            if let Ok(Some(blob)) = reader.keyed_blobs().get(b"picture.png").await {
                assert_eq!(blob, b"not really a png");
                break;
            }
            if n_loops > 25 {
                panic!("reader never saw the writer's blob");
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
            n_loops += 1;
        }
        Ok(())
    }
}
