//! Port of [`OrderedHyperbee`] from `js/hyper-rss/peer/src/feed.js`.
//!
//! A key-value store, built on [`Hyperbee`], that preserves insertion order and stores
//! separate feed-level metadata. Three namespaces (`key`, `order`, `metadata`) live inside one
//! [`Hyperbee`] via [`Hyperbee::sub`]. `order` maps a zero-padded insertion index to an item's
//! `key`, since a [`Hyperbee`] is ordered by key bytes, not insertion time.

use std::sync::Arc;

use futures_lite::{Stream, StreamExt};
use hyperbee::{
    Hyperbee, HyperbeeError,
    prefixed::{Prefixed, PrefixedConfig},
    traverse::{TraverseConfigBuilder, TraverseConfigBuilderError},
};
use hypercore_handshake::CipherTrait;
use thiserror::Error;

use crate::const_::RSS_METADATA_FIELDS;

const KEY_NAMESPACE: &[u8] = b"key";
const ORDER_NAMESPACE: &[u8] = b"order";
const METADATA_NAMESPACE: &[u8] = b"metadata";

/// Errors produced by [`OrderedHyperbee`].
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum FeedError {
    /// An error from the underlying [`Hyperbee`].
    #[error("error from hyperbee: {0}")]
    Hyperbee(#[from] HyperbeeError),
    /// An error building a [`hyperbee::traverse::TraverseConfig`].
    #[error("error building traverse config: {0}")]
    TraverseConfigBuilder(#[from] TraverseConfigBuilderError),
    /// An `order` namespace key was not a valid zero-padded decimal index.
    #[error("order key {0:?} is not a valid order index")]
    InvalidOrderKey(Vec<u8>),
    /// An `order` namespace entry had no associated item key.
    #[error("order entry at index [{0}] has no associated item key")]
    MissingOrderValue(u64),
    /// An `order` namespace entry pointed at a key missing from the `key` namespace.
    #[error("order entry pointed at key {0:?} which is missing from the key namespace")]
    MissingKeyEntry(Vec<u8>),
}

/// A single entry read from [`OrderedHyperbee::get_feed_stream`].
#[derive(Clone, Debug)]
pub struct FeedEntry {
    /// Index of the block, within the underlying Hypercore, where this entry's value is stored.
    pub seq: u64,
    /// The item's key (the hash used to dedupe and order it).
    pub key: Vec<u8>,
    /// The item's value.
    pub value: Option<Vec<u8>>,
    /// Position of this entry in insertion order (`0` is the first item ever inserted).
    pub order_index: u64,
}

fn order_string_from_number(n: u64) -> Vec<u8> {
    format!("{n:06}").into_bytes()
}

fn number_from_order_string(bytes: &[u8]) -> Result<u64, FeedError> {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .ok_or_else(|| FeedError::InvalidOrderKey(bytes.to_vec()))
}

/// A key value store that preserves insertion order, with separate storage for feed-level
/// metadata. See the [module docs](self) for how this is laid out on top of [`Hyperbee`].
#[derive(Debug)]
pub struct OrderedHyperbee {
    bee: Hyperbee,
}

impl OrderedHyperbee {
    /// Wrap a [`Hyperbee`] as an [`OrderedHyperbee`].
    pub fn new(bee: Hyperbee) -> Self {
        Self { bee }
    }

    /// The number of blocks in the underlying Hypercore.
    pub async fn version(&self) -> u64 {
        self.bee.version().await
    }

    /// Add a replication stream for the underlying Hypercore.
    pub async fn add_stream(&self, stream: impl CipherTrait + 'static) -> Result<(), FeedError> {
        Ok(self.bee.add_stream(stream).await?)
    }

    fn key_sub(&self) -> Prefixed {
        self.bee.sub(KEY_NAMESPACE, PrefixedConfig::default())
    }

    fn order_sub(&self) -> Prefixed {
        self.bee.sub(ORDER_NAMESPACE, PrefixedConfig::default())
    }

    fn metadata_sub(&self) -> Prefixed {
        self.bee.sub(METADATA_NAMESPACE, PrefixedConfig::default())
    }

    /// Whether `key` is already present in the feed.
    pub async fn has_key(&self, key: &[u8]) -> Result<bool, FeedError> {
        Ok(self.key_sub().get(key).await?.is_some())
    }

    /// The next insertion index to use, one greater than the highest index currently stored, or
    /// `0` if the feed is empty.
    pub async fn get_next_highest(&self) -> Result<u64, FeedError> {
        let conf = TraverseConfigBuilder::default().reversed(true).build()?;
        let mut stream = Box::pin(self.order_sub().traverse(&conf).await?);
        match stream.next().await {
            None => Ok(0),
            Some(kv) => Ok(number_from_order_string(&kv?.key)? + 1),
        }
    }

    async fn put_next_in_order(&self, item_key: &[u8]) -> Result<(), FeedError> {
        let highest = self.get_next_highest().await?;
        let order_key = order_string_from_number(highest);
        self.order_sub().put(&order_key, Some(item_key)).await?;
        Ok(())
    }

    /// Insert `value` under `key`, and record `key` as the next item in insertion order.
    pub async fn put_ordered_item(&self, key: &[u8], value: &[u8]) -> Result<(), FeedError> {
        self.put_next_in_order(key).await?;
        self.key_sub().put(key, Some(value)).await?;
        Ok(())
    }

    /// Stream the feed in reverse insertion order (newest first).
    pub async fn get_feed_stream<'a>(
        &self,
    ) -> Result<impl Stream<Item = Result<FeedEntry, FeedError>> + 'a, FeedError> {
        let conf = TraverseConfigBuilder::default().reversed(true).build()?;
        let order_stream = self.order_sub().traverse(&conf).await?;
        let key_sub = Arc::new(self.key_sub());
        Ok(order_stream.then(move |item| {
            let key_sub = key_sub.clone();
            async move {
                let kv = item?;
                let order_index = number_from_order_string(&kv.key)?;
                let item_key = kv.value.ok_or(FeedError::MissingOrderValue(order_index))?;
                let (seq, value) = key_sub
                    .get(&item_key)
                    .await?
                    .ok_or_else(|| FeedError::MissingKeyEntry(item_key.clone()))?;
                Ok(FeedEntry {
                    seq,
                    key: item_key,
                    value,
                    order_index,
                })
            }
        }))
    }

    /// Unconditionally set a feed-level metadata field.
    pub async fn put_metadata(&self, key: &[u8], value: &[u8]) -> Result<(), FeedError> {
        self.metadata_sub().put(key, Some(value)).await?;
        Ok(())
    }

    /// Set a feed-level metadata field only if it is unset or its value would change. Returns
    /// whether a new value was written.
    pub async fn maybe_update_metadata(&self, key: &[u8], value: &[u8]) -> Result<bool, FeedError> {
        let (_, new_seq) = self
            .metadata_sub()
            .put_compare_and_swap(key, Some(value), |prev, next| match prev {
                None => true,
                Some(prev) => prev.value.as_deref() != next.value.as_deref(),
            })
            .await?;
        Ok(new_seq.is_some())
    }

    /// Get the value of a feed-level metadata field.
    pub async fn get_metadata_value(&self, key: &[u8]) -> Result<Option<Vec<u8>>, FeedError> {
        Ok(self.metadata_sub().get(key).await?.and_then(|(_, v)| v))
    }

    /// Get all feed-level metadata fields named in [`RSS_METADATA_FIELDS`].
    pub async fn get_metadata(&self) -> Result<Vec<(&'static str, Option<Vec<u8>>)>, FeedError> {
        let mut out = Vec::with_capacity(RSS_METADATA_FIELDS.len());
        for field in RSS_METADATA_FIELDS {
            let value = self.get_metadata_value(field.as_bytes()).await?;
            out.push((field, value));
        }
        Ok(out)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use futures_lite::StreamExt;

    async fn new_feed() -> OrderedHyperbee {
        OrderedHyperbee::new(
            Hyperbee::from_ram()
                .await
                .expect("build in-memory Hyperbee"),
        )
    }

    /// Port of the JS test "OrderedHyperbee is ordered" from
    /// `js/hyper-rss/peer/src/test.js`.
    #[tokio::test]
    async fn ordered_hyperbee_is_ordered() {
        let feed = new_feed().await;

        feed.put_ordered_item(b"100", b"hundred").await.unwrap();
        feed.put_ordered_item(b"001", b"one").await.unwrap();
        feed.put_ordered_item(b"010", b"ten").await.unwrap();

        let mut stream = Box::pin(feed.get_feed_stream().await.unwrap());

        let entry = stream.next().await.unwrap().unwrap();
        assert_eq!(entry.order_index, 2);
        assert_eq!(entry.key, b"010");
        assert_eq!(entry.value.as_deref(), Some(&b"ten"[..]));

        let entry = stream.next().await.unwrap().unwrap();
        assert_eq!(entry.order_index, 1);
        assert_eq!(entry.key, b"001");
        assert_eq!(entry.value.as_deref(), Some(&b"one"[..]));

        let entry = stream.next().await.unwrap().unwrap();
        assert_eq!(entry.order_index, 0);
        assert_eq!(entry.key, b"100");
        assert_eq!(entry.value.as_deref(), Some(&b"hundred"[..]));

        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn has_key_reports_presence() {
        let feed = new_feed().await;

        assert!(!feed.has_key(b"missing").await.unwrap());

        feed.put_ordered_item(b"present", b"value").await.unwrap();
        assert!(feed.has_key(b"present").await.unwrap());
        assert!(!feed.has_key(b"missing").await.unwrap());
    }

    #[tokio::test]
    async fn maybe_update_metadata_is_a_no_op_for_unchanged_values() {
        let feed = new_feed().await;

        let wrote = feed.maybe_update_metadata(b"title", b"a").await.unwrap();
        assert!(wrote);
        let version_after_first_write = feed.version().await;

        let wrote_again = feed.maybe_update_metadata(b"title", b"a").await.unwrap();
        assert!(!wrote_again);
        assert_eq!(feed.version().await, version_after_first_write);

        let wrote_changed = feed.maybe_update_metadata(b"title", b"b").await.unwrap();
        assert!(wrote_changed);
        assert_eq!(
            feed.get_metadata_value(b"title").await.unwrap().as_deref(),
            Some(&b"b"[..])
        );
    }
}
