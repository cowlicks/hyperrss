//! Rust implementation of hyper-rss: peer-to-peer RSS built on Hypercore.
#![warn(
    unreachable_pub,
    missing_debug_implementations,
    missing_docs,
    redundant_lifetimes,
    unsafe_code,
    non_local_definitions,
    clippy::needless_pass_by_value,
    clippy::needless_pass_by_ref_mut,
    clippy::enum_glob_use
)]

mod const_;
mod feed;
mod item;
mod keyed_blobs;
mod peer;
mod swarm;
mod writer;

pub use feed::{FeedEntry, FeedError, OrderedHyperbee};
pub use item::{ParsedFeed, RssItem, DEFAULT_KEY_FIELD};
pub use keyed_blobs::{KeyedBlobs, KeyedBlobsError};
pub use peer::{
    Peer, PeerError, PeerKeys, decode_public_key, encode_public_key, topic_for_keys_core,
};
pub use swarm::DiscoveryOptions;
// Re-exported so downstream crates can build and configure a swarm without depending on
// `hyperswarm` themselves.
pub use hyperswarm::{DhtConfig, Swarm, SwarmConfig, Topic};
pub use writer::NewItem;
