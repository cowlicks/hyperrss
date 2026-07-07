//! A single peer holding one named feed core, backed by [`Corestore`].
//!
//! Unlike the JS implementation (which has separate `Writer`/`Reader` classes), this ports both
//! into one [`Peer`]: it's a writer when the underlying Hypercore's key pair carries a secret
//! key, and a read-only reader otherwise.

use corestore::Corestore;
use hyperbee::Hyperbee;
use hypercore::{PartialKeypair, VerifyingKey};
use hypercore_handshake::CipherTrait;
use thiserror::Error;

use crate::{feed::FeedError, OrderedHyperbee};

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
}

/// A peer for a single hyper-rss feed. Acts as a writer when it holds the feed core's secret
/// key, and a reader otherwise. See the [module docs](self) for why this replaces JS's separate
/// `Writer`/`Reader` types.
#[derive(Debug)]
pub struct Peer {
    key_pair: PartialKeypair,
    feed: OrderedHyperbee,
}

impl Peer {
    /// Create a writer: creates (or opens) a locally-owned, writable named core in `store`.
    pub async fn writer(store: &Corestore, name: &str) -> Result<Self, PeerError> {
        let hc = store.get_from_name(name).await?;
        Self::from_hypercore(hc)
    }

    /// Create a reader: opens a read-only core in `store` by its public key.
    pub async fn reader(store: &Corestore, key: VerifyingKey) -> Result<Self, PeerError> {
        let hc = store.get_from_verifying_key(&key).await?;
        Self::from_hypercore(hc)
    }

    fn from_hypercore(hc: hypercore::Hypercore) -> Result<Self, PeerError> {
        let key_pair = hc.key_pair();
        let feed = OrderedHyperbee::new(Hyperbee::from_hypercore(hc)?);
        Ok(Self { key_pair, feed })
    }

    /// Whether this peer can append to the feed (it holds the feed core's secret key).
    pub fn is_writer(&self) -> bool {
        self.key_pair.secret.is_some()
    }

    /// The feed core's public key.
    pub fn public_key(&self) -> VerifyingKey {
        self.key_pair.public
    }

    /// Add a replication stream for the feed core.
    pub async fn add_stream(&self, stream: impl CipherTrait + 'static) -> Result<(), PeerError> {
        Ok(self.feed.add_stream(stream).await?)
    }

    /// The peer's feed.
    pub fn feed(&self) -> &OrderedHyperbee {
        &self.feed
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use hypercore_handshake::{
        state_machine::{hc_specific::generate_keypair, SecStream},
        Cipher,
    };
    use std::time::Duration;
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

    #[tokio::test]
    async fn writer_is_writer_reader_is_not() -> Result<(), PeerError> {
        let store_a = Corestore::new_mem().await;
        let store_b = Corestore::new_mem().await;
        let writer = Peer::writer(&store_a, "feed").await?;
        assert!(writer.is_writer());

        let reader = Peer::reader(&store_b, writer.public_key()).await?;
        assert!(!reader.is_writer());
        Ok(())
    }

    #[tokio::test]
    async fn writer_and_reader_replicate_a_feed() -> Result<(), Box<dyn std::error::Error>> {
        let store_a = Corestore::new_mem().await;
        let store_b = Corestore::new_mem().await;

        let writer = Peer::writer(&store_a, "feed").await?;
        let reader = Peer::reader(&store_b, writer.public_key()).await?;

        let (a_to_b, b_to_a) = create_connected_streams();
        writer.add_stream(a_to_b).await?;
        reader.add_stream(b_to_a).await?;

        writer.feed().put_ordered_item(b"hello", b"world").await?;

        let mut n_loops = 0;
        loop {
            // Block data can arrive slightly after the metadata that references it, so treat
            // errors the same as "not yet" and keep retrying, matching `bee`'s own tests.
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
}
