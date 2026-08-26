//! The feed-item model shared by the writer methods and, later, the RSS ingestion pipeline.
//!
//! JS stores each item as `JSON.stringify(handled)` (`writer.js`'s `addNewItems`), where
//! `handled` is the whole `rss-parser` item object with its media URLs rewritten — every field
//! the parser produced is preserved, not just the ones hyper-rss knows about. [`RssItem`] keeps
//! that property by being a JSON object rather than a fixed struct, so an item round-trips
//! through the feed unchanged and stays readable by the JS aggregator.
//!
//! Nothing here fetches or parses anything: an [`RssItem`] is whatever produced it (a test, an
//! RSS parser, another peer's feed), which is what lets the writer methods work without an HTTP
//! client.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// The item field used as the feed key by default, matching JS's
/// `defaultRssItemHasher = ({ guid }) => guid` (`items.js`).
pub const DEFAULT_KEY_FIELD: &str = "guid";

/// The item field holding the enclosure object (`{ url, length, type }`).
const ENCLOSURE_FIELD: &str = "enclosure";

/// The field, within an enclosure object, holding its URL.
const URL_FIELD: &str = "url";

/// The item field holding the item's HTML body.
const CONTENT_FIELD: &str = "content";

/// A single feed item, as a JSON object.
///
/// See the [module docs](self) for why this is an open JSON object rather than a fixed struct.
/// It serializes and deserializes as the bare object, so `serde_json::to_vec(&item)` produces
/// exactly what JS's `JSON.stringify(item)` would.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RssItem(Map<String, Value>);

impl RssItem {
    /// An item with no fields.
    pub fn new() -> Self {
        Self(Map::new())
    }

    /// The item's fields.
    pub fn fields(&self) -> &Map<String, Value> {
        &self.0
    }

    /// The item's fields, mutably.
    pub fn fields_mut(&mut self) -> &mut Map<String, Value> {
        &mut self.0
    }

    /// The value of one field.
    pub fn get(&self, field: &str) -> Option<&Value> {
        self.0.get(field)
    }

    /// Set one field, returning the value it replaced.
    pub fn insert(&mut self, field: impl Into<String>, value: Value) -> Option<Value> {
        self.0.insert(field.into(), value)
    }

    /// The value of one field, as a string (`None` if absent or not a JSON string).
    fn get_str(&self, field: &str) -> Option<&str> {
        self.0.get(field)?.as_str()
    }

    /// The item's `guid` — the default feed key. See [`DEFAULT_KEY_FIELD`].
    pub fn guid(&self) -> Option<&str> {
        self.get_str(DEFAULT_KEY_FIELD)
    }

    /// The key this item is stored under: its [`guid`](Self::guid) as bytes.
    ///
    /// `None` when the item has no `guid`, or it isn't a string. JS silently turns that case into
    /// an `undefined` key; the writer methods reject it instead. Callers keying off a different
    /// field should use [`Peer::get_missing_with`](crate::Peer::get_missing_with).
    pub fn default_key(&self) -> Option<&[u8]> {
        self.guid().map(str::as_bytes)
    }

    /// The item's HTML body (JS `item.content`, the field `itemImgHandler` rewrites `<img src>`
    /// within).
    pub fn content(&self) -> Option<&str> {
        self.get_str(CONTENT_FIELD)
    }

    /// Set the item's HTML body.
    pub fn set_content(&mut self, content: impl Into<String>) {
        self.insert(CONTENT_FIELD, Value::String(content.into()));
    }

    /// The item's enclosure URL (the podcast/audio/video attachment), if it has an enclosure.
    pub fn enclosure_url(&self) -> Option<&str> {
        self.0.get(ENCLOSURE_FIELD)?.get(URL_FIELD)?.as_str()
    }

    /// Point the item's enclosure at `url`, returning whether there was an enclosure to rewrite.
    ///
    /// Mirrors JS `itemEnclosureHandler` (`items.js`), which leaves an item with no enclosure
    /// untouched rather than creating one.
    pub fn set_enclosure_url(&mut self, url: impl Into<String>) -> bool {
        let Some(enclosure) = self.0.get_mut(ENCLOSURE_FIELD).and_then(Value::as_object_mut) else {
            return false;
        };
        enclosure.insert(URL_FIELD.to_string(), Value::String(url.into()));
        true
    }
}

impl From<Map<String, Value>> for RssItem {
    fn from(fields: Map<String, Value>) -> Self {
        Self(fields)
    }
}

impl From<RssItem> for Map<String, Value> {
    fn from(item: RssItem) -> Self {
        item.0
    }
}

/// A parsed feed: its feed-level fields plus its items.
///
/// This is the input to [`Peer::update_feed`](crate::Peer::update_feed), and the type the RSS
/// ingestion pipeline will eventually produce. It deliberately says nothing about where the feed
/// came from.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParsedFeed {
    /// Feed-level fields (`title`, `description`, `pubDate`, `lastBuildDate`, ...).
    #[serde(flatten)]
    fields: Map<String, Value>,
    /// The feed's items, newest first.
    #[serde(default)]
    items: Vec<RssItem>,
}

impl ParsedFeed {
    /// A feed with the given feed-level fields and items.
    ///
    /// `items` must be ordered newest first — see [`Self::items`].
    pub fn new(fields: Map<String, Value>, items: Vec<RssItem>) -> Self {
        Self { fields, items }
    }

    /// A feed with items but no feed-level metadata.
    pub fn from_items(items: Vec<RssItem>) -> Self {
        Self::new(Map::new(), items)
    }

    /// The feed-level fields.
    pub fn fields(&self) -> &Map<String, Value> {
        &self.fields
    }

    /// The value of one feed-level field.
    pub fn field(&self, name: &str) -> Option<&Value> {
        self.fields.get(name)
    }

    /// Set one feed-level field, returning the value it replaced.
    pub fn insert_field(&mut self, name: impl Into<String>, value: Value) -> Option<Value> {
        self.fields.insert(name.into(), value)
    }

    /// The feed's items, **newest first**.
    ///
    /// This is the order `rss-parser` produces, and the reason
    /// [`Peer::get_missing`](crate::Peer::get_missing) iterates in reverse: items must be appended
    /// to the feed oldest first so that insertion order matches publication order.
    pub fn items(&self) -> &[RssItem] {
        &self.items
    }

    /// The feed's items, mutably.
    pub fn items_mut(&mut self) -> &mut Vec<RssItem> {
        &mut self.items
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::json;

    fn item(value: Value) -> RssItem {
        serde_json::from_value(value).expect("an object deserializes as an RssItem")
    }

    #[test]
    fn an_item_round_trips_every_field() {
        let original = json!({
            "guid": "abc",
            "title": "A post",
            "unknownFieldHyperRssNeverHeardOf": [1, 2, 3],
        });
        let item = item(original.clone());
        assert_eq!(item.guid(), Some("abc"));
        assert_eq!(
            serde_json::to_value(&item).unwrap(),
            original,
            "serializing must reproduce the input object exactly, so the JS aggregator can read it"
        );
    }

    #[test]
    fn default_key_is_the_guid() {
        assert_eq!(item(json!({"guid": "abc"})).default_key(), Some(&b"abc"[..]));
        assert_eq!(item(json!({"title": "no guid"})).default_key(), None);
        assert_eq!(
            item(json!({"guid": 7})).default_key(),
            None,
            "a non-string guid has no byte key"
        );
    }

    #[test]
    fn set_enclosure_url_rewrites_only_an_existing_enclosure() {
        let mut with = item(json!({
            "guid": "a",
            "enclosure": {"url": "https://example.com/ep.mp3", "type": "audio/mpeg"},
        }));
        assert!(with.set_enclosure_url("hash.mp3"));
        assert_eq!(with.enclosure_url(), Some("hash.mp3"));
        assert_eq!(
            with.get("enclosure").unwrap().get("type").unwrap(),
            "audio/mpeg",
            "rewriting the URL must not drop the enclosure's other fields"
        );

        let mut without = item(json!({"guid": "a"}));
        assert!(!without.set_enclosure_url("hash.mp3"));
        assert_eq!(without.get("enclosure"), None);
    }

    #[test]
    fn a_parsed_feed_keeps_feed_level_fields_beside_its_items() {
        let feed: ParsedFeed = serde_json::from_value(json!({
            "title": "My feed",
            "lastBuildDate": "Mon, 01 Jan 2024 00:00:00 GMT",
            "items": [{"guid": "b"}, {"guid": "a"}],
        }))
        .unwrap();
        assert_eq!(feed.field("title").unwrap(), "My feed");
        assert_eq!(feed.items().len(), 2);
        assert_eq!(feed.items()[0].guid(), Some("b"), "items are newest first");
    }
}
