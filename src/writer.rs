//! Writer-side orchestration: the port of JS `writer.js`'s `getMissing` / `addNewItems` /
//! `updateMetadata` / `updateFeed`, as methods on the unified [`Peer`].
//!
//! These take an already-parsed [`ParsedFeed`] rather than a URL, so they carry no dependency on
//! an HTTP client or an RSS parser — fetching and parsing is a separate layer that produces the
//! [`ParsedFeed`] these consume. JS couples the two (`Writer.init` calls `parser.parseURL` and
//! stashes the result on `this`); splitting them is what lets the whole write path be tested with
//! hand-built items and no network.
//!
//! What is stored stays wire-compatible with JS: each item is its full JSON object, and each
//! metadata field is its JSON-encoded value (so a string field is stored *with* its quotes).

use std::collections::HashSet;

use crate::{
    const_::RSS_METADATA_FIELDS,
    item::{ParsedFeed, RssItem},
    peer::{Peer, PeerError},
};

/// An item that is not yet in the feed, paired with the key it will be stored under.
///
/// The output of [`Peer::get_missing`] and the input to [`Peer::add_new_items`], mirroring the
/// `{ key, rssItem }` pairs JS's `itemsNotHyperized` produces (`items.js`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewItem {
    /// The feed key for this item — by default its `guid`.
    pub key: Vec<u8>,
    /// The item itself.
    pub item: RssItem,
}

impl Peer {
    /// The items in `feed` that this peer's stored feed doesn't have yet, **oldest first**, keyed
    /// by each item's `guid`.
    ///
    /// See [`Peer::get_missing_with`] for the details and for keying off a different field.
    pub async fn get_missing(&self, feed: &ParsedFeed) -> Result<Vec<NewItem>, PeerError> {
        self.get_missing_with(feed, |item| item.default_key().map(<[u8]>::to_vec))
            .await
    }

    /// The items in `feed` that this peer's stored feed doesn't have yet, **oldest first**, keyed
    /// by `key_of`.
    ///
    /// Port of JS's `itemsNotHyperized` (`items.js`) with its `hasher` option. The reversal
    /// matters: [`ParsedFeed::items`] are newest first, and appending them oldest first is what
    /// makes the feed's insertion order match publication order.
    ///
    /// Returns [`PeerError::ItemMissingKey`] if `key_of` yields no key for an item — JS instead
    /// lets an `undefined` key through and stores the item under it.
    ///
    /// Items whose key repeats within `feed` are reported once, unlike JS: a repeat is invisible
    /// to the [`has_key`](crate::OrderedHyperbee::has_key) check (nothing is stored yet), so
    /// passing both to [`add_new_items`](Self::add_new_items) would append two insertion-order
    /// entries pointing at one key.
    pub async fn get_missing_with<F>(
        &self,
        feed: &ParsedFeed,
        key_of: F,
    ) -> Result<Vec<NewItem>, PeerError>
    where
        F: Fn(&RssItem) -> Option<Vec<u8>>,
    {
        let mut out = Vec::new();
        let mut seen = HashSet::new();
        for (index, item) in feed.items().iter().enumerate().rev() {
            let key = key_of(item).ok_or(PeerError::ItemMissingKey(index))?;
            if !seen.insert(key.clone()) {
                continue;
            }
            if !self.feed().has_key(&key).await? {
                out.push(NewItem {
                    key,
                    item: item.clone(),
                });
            }
        }
        Ok(out)
    }

    /// Append `new_items` to the feed in order, each stored as its JSON object.
    ///
    /// Port of JS's `addNewItems`. JS additionally runs each item through `handleItem` here, to
    /// download its enclosure and inline images into the keyed blobs and rewrite their URLs; that
    /// step needs the downloader from the not-yet-ported ingestion pipeline, so items are
    /// currently stored as given.
    pub async fn add_new_items(&self, new_items: &[NewItem]) -> Result<(), PeerError> {
        self.writable()?;
        for NewItem { key, item } in new_items {
            let value = serde_json::to_vec(item)?;
            self.feed().put_ordered_item(key, &value).await?;
        }
        Ok(())
    }

    /// Copy `feed`'s feed-level metadata into the stored feed, writing only the four
    /// `RSS_METADATA_FIELDS` (`title`, `description`, `pubDate`, `lastBuildDate`) and only those
    /// `feed` actually has. Returns how many fields changed.
    ///
    /// Port of JS's `updateMetadata`. Each value is stored JSON-encoded, matching JS's
    /// `JSON.stringify(value)`, so a string field is stored with its surrounding quotes.
    pub async fn update_metadata(&self, feed: &ParsedFeed) -> Result<usize, PeerError> {
        self.writable()?;
        let mut changed = 0;
        for field in RSS_METADATA_FIELDS {
            let Some(value) = feed.field(field) else {
                continue;
            };
            let encoded = serde_json::to_vec(value)?;
            if self
                .feed()
                .maybe_update_metadata(field.as_bytes(), &encoded)
                .await?
            {
                changed += 1;
            }
        }
        Ok(changed)
    }

    /// Bring the stored feed up to date with `feed`: append every item not already stored, then
    /// refresh the feed-level metadata. Returns the items that were appended.
    ///
    /// Port of JS's `updateFeed`. Calling it twice with the same [`ParsedFeed`] appends nothing
    /// the second time and returns an empty `Vec`.
    pub async fn update_feed(&self, feed: &ParsedFeed) -> Result<Vec<NewItem>, PeerError> {
        self.writable()?;
        let missing = self.get_missing(feed).await?;
        self.add_new_items(&missing).await?;
        self.update_metadata(feed).await?;
        Ok(missing)
    }

    /// `Ok(())` if this peer holds the feed's secret key, [`PeerError::NotAWriter`] otherwise.
    fn writable(&self) -> Result<(), PeerError> {
        if self.is_writer() {
            Ok(())
        } else {
            Err(PeerError::NotAWriter)
        }
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use corestore::Corestore;
    use futures_lite::StreamExt;
    use serde_json::{Value, json};

    use super::*;
    use crate::peer::test::connected_writer_and_reader;

    /// A feed of `guid`-keyed items, newest first, as `rss-parser` would yield them.
    fn feed_of(guids: &[&str]) -> ParsedFeed {
        ParsedFeed::from_items(
            guids
                .iter()
                .map(|guid| serde_json::from_value(json!({"guid": guid})).unwrap())
                .collect(),
        )
    }

    async fn writer() -> Result<Peer, Box<dyn std::error::Error>> {
        let store = Corestore::new_mem().await;
        Ok(Peer::writer(&store, "feed").await?)
    }

    /// The stored feed's item keys, newest first (the order `get_feed_stream` yields).
    async fn stored_keys(peer: &Peer) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let mut stream = Box::pin(peer.feed().get_feed_stream().await?);
        let mut out = Vec::new();
        while let Some(entry) = stream.next().await {
            out.push(String::from_utf8(entry?.key)?);
        }
        Ok(out)
    }

    #[tokio::test]
    async fn get_missing_returns_every_item_oldest_first() -> Result<(), Box<dyn std::error::Error>>
    {
        let writer = writer().await?;
        let missing = writer
            .get_missing(&feed_of(&["newest", "middle", "oldest"]))
            .await?;

        let keys: Vec<_> = missing.iter().map(|n| n.key.clone()).collect();
        assert_eq!(
            keys,
            vec![b"oldest".to_vec(), b"middle".to_vec(), b"newest".to_vec()],
            "a newest-first feed must come back oldest-first, ready to append in order"
        );
        Ok(())
    }

    #[tokio::test]
    async fn get_missing_skips_items_already_stored() -> Result<(), Box<dyn std::error::Error>> {
        let writer = writer().await?;
        writer.update_feed(&feed_of(&["b", "a"])).await?;

        let missing = writer.get_missing(&feed_of(&["c", "b", "a"])).await?;
        assert_eq!(missing.len(), 1);
        assert_eq!(missing[0].key, b"c".to_vec());
        Ok(())
    }

    #[tokio::test]
    async fn get_missing_reports_a_repeated_key_once() -> Result<(), Box<dyn std::error::Error>> {
        let writer = writer().await?;
        let missing = writer.get_missing(&feed_of(&["dup", "dup"])).await?;
        assert_eq!(missing.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn get_missing_rejects_an_item_with_no_key() -> Result<(), Box<dyn std::error::Error>> {
        let writer = writer().await?;
        let feed = ParsedFeed::from_items(vec![
            serde_json::from_value(json!({"guid": "has one"}))?,
            serde_json::from_value(json!({"title": "has none"}))?,
        ]);

        match writer.get_missing(&feed).await {
            Err(PeerError::ItemMissingKey(index)) => assert_eq!(index, 1),
            other => panic!("expected ItemMissingKey, got {other:?}"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn add_new_items_stores_whole_items_in_order() -> Result<(), Box<dyn std::error::Error>> {
        let writer = writer().await?;
        let item: RssItem = serde_json::from_value(json!({
            "guid": "a",
            "title": "A post",
            "extra": {"nested": true},
        }))?;
        let feed = ParsedFeed::from_items(vec![
            serde_json::from_value(json!({"guid": "b"}))?,
            item.clone(),
        ]);

        writer
            .add_new_items(&writer.get_missing(&feed).await?)
            .await?;

        assert_eq!(
            stored_keys(&writer).await?,
            vec!["b".to_string(), "a".to_string()],
            "newest first on the way out means 'b', appended second, comes first"
        );

        let mut stream = Box::pin(writer.feed().get_feed_stream().await?);
        let newest = stream.next().await.unwrap()?;
        let oldest = stream.next().await.unwrap()?;
        assert_eq!(newest.key, b"b");
        let stored: RssItem = serde_json::from_slice(&oldest.value.expect("item has a value"))?;
        assert_eq!(stored, item, "every field must survive the round trip");
        Ok(())
    }

    #[tokio::test]
    async fn update_metadata_writes_known_fields_json_encoded()
    -> Result<(), Box<dyn std::error::Error>> {
        let writer = writer().await?;
        let mut feed = feed_of(&[]);
        feed.insert_field("title", json!("My feed"));
        feed.insert_field("description", json!("Words"));
        feed.insert_field("someOtherField", json!("ignored"));

        assert_eq!(writer.update_metadata(&feed).await?, 2);
        assert_eq!(
            writer.feed().get_metadata_value(b"title").await?.as_deref(),
            Some(&br#""My feed""#[..]),
            "values are JSON-encoded, matching JS's JSON.stringify"
        );
        assert_eq!(
            writer.feed().get_metadata_value(b"someOtherField").await?,
            None,
            "only RSS_METADATA_FIELDS are stored"
        );
        assert_eq!(
            writer.feed().get_metadata_value(b"pubDate").await?,
            None,
            "a field the feed doesn't have is not written"
        );

        assert_eq!(
            writer.update_metadata(&feed).await?,
            0,
            "re-writing identical metadata must change nothing"
        );
        Ok(())
    }

    #[tokio::test]
    async fn update_feed_is_idempotent() -> Result<(), Box<dyn std::error::Error>> {
        let writer = writer().await?;
        let mut feed = feed_of(&["b", "a"]);
        feed.insert_field("title", json!("My feed"));

        assert_eq!(writer.update_feed(&feed).await?.len(), 2);
        let version = writer.feed().version().await;

        assert!(writer.update_feed(&feed).await?.is_empty());
        assert_eq!(
            writer.feed().version().await,
            version,
            "a second update with an unchanged feed must not append a block"
        );
        Ok(())
    }

    #[tokio::test]
    async fn update_feed_appends_only_what_is_new() -> Result<(), Box<dyn std::error::Error>> {
        let writer = writer().await?;
        writer.update_feed(&feed_of(&["b", "a"])).await?;

        let appended = writer.update_feed(&feed_of(&["d", "c", "b", "a"])).await?;
        assert_eq!(
            appended.iter().map(|n| n.key.clone()).collect::<Vec<_>>(),
            vec![b"c".to_vec(), b"d".to_vec()],
            "new items are appended oldest first"
        );
        assert_eq!(stored_keys(&writer).await?, vec!["d", "c", "b", "a"]);
        Ok(())
    }

    #[tokio::test]
    async fn a_reader_cannot_write() -> Result<(), Box<dyn std::error::Error>> {
        let (_writer, reader) = connected_writer_and_reader().await?;
        let feed = feed_of(&["a"]);

        assert!(matches!(
            reader.update_feed(&feed).await,
            Err(PeerError::NotAWriter)
        ));
        assert!(matches!(
            reader.add_new_items(&[]).await,
            Err(PeerError::NotAWriter)
        ));
        assert!(matches!(
            reader.update_metadata(&feed).await,
            Err(PeerError::NotAWriter)
        ));
        assert!(
            reader.get_missing(&feed).await.is_ok(),
            "reading what's missing is not a write"
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_written_feed_replicates_to_a_reader() -> Result<(), Box<dyn std::error::Error>> {
        let (writer, reader) = connected_writer_and_reader().await?;
        let mut feed = feed_of(&["newest", "oldest"]);
        feed.insert_field("title", json!("My feed"));
        writer.update_feed(&feed).await?;

        let mut n_loops = 0;
        loop {
            if let Ok(true) = reader.feed().has_key(b"newest").await {
                break;
            }
            if n_loops > 25 {
                panic!("reader never saw the writer's items");
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
            n_loops += 1;
        }

        assert_eq!(stored_keys(&reader).await?, vec!["newest", "oldest"]);
        let title = reader.feed().get_metadata_value(b"title").await?;
        assert_eq!(
            serde_json::from_slice::<Value>(&title.expect("title replicated"))?,
            json!("My feed")
        );
        Ok(())
    }
}
