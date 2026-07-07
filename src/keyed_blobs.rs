//! Port of [`KeyedBlobs`] from `js/hyper-rss/peer/src/blobs.js`.
//!
//! Pairs a `blobKeys` [`Hyperbee`] (an index mapping an application key to a [`PutBlob`] id) with
//! a `blobs` [`Hyperblobs`] (the raw, block-chunked byte storage the id points into). `blobKeys`
//! never holds blob bytes, only the id needed to locate them in `blobs`.

use corestore::Corestore;
use futures_lite::{Stream, StreamExt};
use hyperbee::{traverse::TraverseConfig, Hyperbee, HyperbeeError};
use hyperblobs::{
    GetOptions, Hyperblobs, HyperblobsBuilder, HyperblobsBuilderError, PutBlob, PutOptions,
};
use hypercore::Hypercore;
use hypercore_handshake::CipherTrait;
use thiserror::Error;

/// Errors produced by [`KeyedBlobs`].
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum KeyedBlobsError {
    /// An error from the `blobKeys` [`Hyperbee`].
    #[error("error from hyperbee: {0}")]
    Hyperbee(#[from] HyperbeeError),
    /// An error from the `blobs` [`Hyperblobs`].
    #[error("error from hyperblobs: {0}")]
    Hyperblobs(#[from] hyperblobs::Error),
    /// An error building [`Hyperblobs`].
    #[error("error building hyperblobs: {0}")]
    HyperblobsBuilder(#[from] HyperblobsBuilderError),
    /// An error from [`Corestore`].
    #[error("error from corestore: {0}")]
    Corestore(#[from] corestore::Error),
    /// A stored blob id failed to decode as JSON.
    #[error("error decoding blob id for key {0:?}: {1}")]
    Decode(Vec<u8>, serde_json::Error),
    /// A key in `blobKeys` had no associated blob id.
    #[error("key {0:?} has no associated blob id")]
    MissingBlobId(Vec<u8>),
}

/// A content-addressed, keyed blob store built on one `blobKeys` [`Hyperbee`] and one `blobs`
/// [`Hyperblobs`]. See the [module docs](self) for the storage layout.
#[derive(Debug)]
pub struct KeyedBlobs {
    keys: Hyperbee,
    blobs: Hyperblobs,
}

impl KeyedBlobs {
    /// Wrap a `blobKeys` and `blobs` [`Hypercore`] as a [`KeyedBlobs`].
    pub fn from_hypercores(blob_keys: Hypercore, blobs: Hypercore) -> Result<Self, KeyedBlobsError> {
        let keys = Hyperbee::from_hypercore(blob_keys)?;
        let blobs = HyperblobsBuilder::default().core(blobs).build()?;
        Ok(Self { keys, blobs })
    }

    /// Open the `blobKeys` and `blobs` cores (named `"blobKeys"` and `"blobs"`) from `store`,
    /// creating them if they don't exist yet.
    pub async fn from_corestore(store: &Corestore) -> Result<Self, KeyedBlobsError> {
        let blob_keys = store.get_from_name("blobKeys").await?;
        let blobs = store.get_from_name("blobs").await?;
        Self::from_hypercores(blob_keys, blobs)
    }

    /// Add a replication stream for the `blobKeys` core.
    pub async fn add_blob_keys_stream(
        &self,
        stream: impl CipherTrait + 'static,
    ) -> Result<(), KeyedBlobsError> {
        Ok(self.keys.add_stream(stream).await?)
    }

    /// Add a replication stream for the `blobs` core.
    pub async fn add_blobs_stream(
        &self,
        stream: impl CipherTrait + 'static,
    ) -> Result<(), KeyedBlobsError> {
        Ok(self.blobs.add_stream(stream).await?)
    }

    fn decode_id(key: &[u8], value: Option<Vec<u8>>) -> Result<PutBlob, KeyedBlobsError> {
        let bytes = value.ok_or_else(|| KeyedBlobsError::MissingBlobId(key.to_vec()))?;
        serde_json::from_slice(&bytes).map_err(|e| KeyedBlobsError::Decode(key.to_vec(), e))
    }

    /// Store `blob` under `key`, unconditionally. Returns the id used to fetch it again.
    pub async fn put(&self, key: &[u8], blob: &[u8]) -> Result<PutBlob, KeyedBlobsError> {
        let id = self.blobs.put(blob, &PutOptions::default()).await?;
        let id_bytes = serde_json::to_vec(&id).expect("PutBlob always serializes");
        self.keys.put(key, Some(&id_bytes)).await?;
        Ok(id)
    }

    /// Store `blob` under `key` only if `key` isn't already present. Returns `None` if `key` was
    /// already present.
    pub async fn maybe_put(
        &self,
        key: &[u8],
        blob: &[u8],
    ) -> Result<Option<PutBlob>, KeyedBlobsError> {
        if self.keys.get(key).await?.is_some() {
            return Ok(None);
        }
        Ok(Some(self.put(key, blob).await?))
    }

    /// Get the id stored under `key`, without fetching the blob's bytes.
    pub async fn get_id(&self, key: &[u8]) -> Result<Option<PutBlob>, KeyedBlobsError> {
        match self.keys.get(key).await? {
            None => Ok(None),
            Some((_, value)) => Some(Self::decode_id(key, value)).transpose(),
        }
    }

    /// Get the blob stored under `key`.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, KeyedBlobsError> {
        match self.get_id(key).await? {
            None => Ok(None),
            Some(id) => Ok(Some(self.blobs.get(&id, &GetOptions::default()).await?)),
        }
    }

    /// Stream all keys in `blobKeys`, in ascending key order.
    pub async fn key_stream<'a>(
        &self,
    ) -> Result<impl Stream<Item = Result<Vec<u8>, KeyedBlobsError>> + 'a, KeyedBlobsError> {
        let stream = self.keys.traverse(TraverseConfig::default()).await?;
        Ok(stream.map(|kv| Ok(kv?.key)))
    }

    /// Collect all keys in `blobKeys`, in ascending key order.
    pub async fn get_keys(&self) -> Result<Vec<Vec<u8>>, KeyedBlobsError> {
        let mut stream = Box::pin(self.key_stream().await?);
        let mut out = vec![];
        while let Some(key) = stream.next().await {
            out.push(key?);
        }
        Ok(out)
    }

    /// Collect every `(key, blob)` pair in the store, in ascending key order.
    pub async fn get_keys_and_blobs(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>, KeyedBlobsError> {
        let mut stream = Box::pin(self.keys.traverse(TraverseConfig::default()).await?);
        let mut out = vec![];
        while let Some(kv) = stream.next().await {
            let kv = kv?;
            let id = Self::decode_id(&kv.key, kv.value)?;
            let blob = self.blobs.get(&id, &GetOptions::default()).await?;
            out.push((kv.key, blob));
        }
        Ok(out)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use hypercore::{HypercoreBuilder, Storage};

    async fn new_keyed_blobs() -> KeyedBlobs {
        let blob_keys = HypercoreBuilder::new(Storage::new_memory().await.unwrap())
            .build()
            .await
            .unwrap();
        let blobs = HypercoreBuilder::new(Storage::new_memory().await.unwrap())
            .build()
            .await
            .unwrap();
        KeyedBlobs::from_hypercores(blob_keys, blobs).unwrap()
    }

    const TEST_KEY: &[u8] = b"foobar";
    const TEST_BLOB: &[u8] = b"Mello wort?";

    /// Port of the JS test "KeyedBlobs.get" from `js/hyper-rss/peer/src/test.js`.
    #[tokio::test]
    async fn get_returns_a_put_blob() {
        let kb = new_keyed_blobs().await;
        kb.put(TEST_KEY, TEST_BLOB).await.unwrap();

        let gotten = kb.get(TEST_KEY).await.unwrap();
        assert_eq!(gotten.as_deref(), Some(TEST_BLOB));
    }

    #[tokio::test]
    async fn get_on_missing_key_is_none() {
        let kb = new_keyed_blobs().await;
        assert!(kb.get(b"missing").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn maybe_put_does_not_overwrite_an_existing_key() {
        let kb = new_keyed_blobs().await;

        let first_id = kb.maybe_put(TEST_KEY, TEST_BLOB).await.unwrap();
        assert!(first_id.is_some());

        let second = kb.maybe_put(TEST_KEY, b"different data").await.unwrap();
        assert!(second.is_none());
        assert_eq!(kb.get(TEST_KEY).await.unwrap().as_deref(), Some(TEST_BLOB));
    }

    #[tokio::test]
    async fn get_keys_and_blobs_collects_everything_stored() {
        let kb = new_keyed_blobs().await;
        kb.put(b"a", b"one").await.unwrap();
        kb.put(b"b", b"two").await.unwrap();

        let keys = kb.get_keys().await.unwrap();
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec()]);

        let keys_and_blobs = kb.get_keys_and_blobs().await.unwrap();
        assert_eq!(
            keys_and_blobs,
            vec![(b"a".to_vec(), b"one".to_vec()), (b"b".to_vec(), b"two".to_vec())]
        );
    }
}
