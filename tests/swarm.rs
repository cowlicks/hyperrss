//! A writer and a reader finding each other over a DHT, with nothing shared between them but
//! a feed id string.
//!
//! The bootstrap node is an in-process Rust `Dht`, so this never touches the network: passing
//! `add_bootstrap_node` makes the config's bootstrap list non-empty, and `hyperdht` consults
//! the public `DEFAULT_BOOTSTRAP` list only when that list is empty.
//!
//! Modelled on `hyperswarm/swarm/tests/integration.rs`'s `replicate_with_hypercore`, which
//! does the same for a single hypercore.

use std::time::{Duration, Instant};

use corestore::Corestore;
use futures_lite::StreamExt;
use hyperdht::adht::Dht;
use hyperrss::{DhtConfig, DiscoveryOptions, Peer, Swarm, decode_public_key};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

/// Budget for anything that depends on discovery or replication. Generous on purpose: a
/// failure here should mean something is broken, not that CI was busy.
const BUDGET: Duration = Duration::from_secs(30);

/// Poll `f` until it reports true, or fail having waited [`BUDGET`].
///
/// An error counts as "not yet, keep waiting", because mid-replication it usually is one:
/// reading a hyperbee whose blocks are still arriving fails with `NoBlockAtSeqError` rather
/// than reporting the key absent. The existing in-memory tests swallow it the same way
/// (`src/peer.rs`'s `if let Ok(true)`). The last error is kept so a genuine timeout says
/// what was actually going wrong.
async fn until(what: &str, mut f: impl AsyncFnMut() -> Result<bool>) -> Result<()> {
    let start = Instant::now();
    let mut last_err = None;
    while start.elapsed() < BUDGET {
        match f().await {
            Ok(true) => return Ok(()),
            Ok(false) => last_err = None,
            Err(e) => last_err = Some(e),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    match last_err {
        Some(e) => panic!("timed out after {BUDGET:?} waiting for {what}; last error: {e}"),
        None => panic!("timed out after {BUDGET:?} waiting for {what}"),
    }
}

/// Start an isolated DHT and return the address to bootstrap against.
async fn bootstrap_node() -> Result<std::net::SocketAddr> {
    let mut bs = Dht::with_config(
        DhtConfig::default()
            .empty_bootstrap_nodes()
            .set_address("127.0.0.1:0".parse()?),
    )
    .await?;
    let addr = bs.local_addr()?;
    tokio::spawn(async move { while bs.next().await.is_some() {} });
    Ok(addr)
}

/// The whole point: a reader given only the `base64url` feed id finds the writer on the DHT,
/// learns the other three cores' keys from its bootstrap entry, and replicates both the feed
/// and the keyed blobs over the one connection discovery produced.
#[tokio::test(flavor = "multi_thread")]
async fn a_reader_finds_a_writer_over_the_dht_given_only_its_feed_id() -> Result<()> {
    let bs_addr = bootstrap_node().await?;
    let config = || DhtConfig::default().add_bootstrap_node(bs_addr);

    // The writer announces and has data before the reader ever looks. `writer_with_swarm`
    // returns only once its first announce round has flushed, so the reader's first lookup
    // cannot miss it.
    let store_a = Corestore::new_mem().await;
    let writer = Peer::writer_with_swarm(
        &store_a,
        "feed",
        Swarm::new(config()).await?,
        DiscoveryOptions::default(),
    )
    .await?;
    writer.feed().put_ordered_item(b"hello", b"world").await?;
    writer
        .keyed_blobs()
        .put(b"picture.png", b"not really a png")
        .await?;

    // Everything the reader is given, as a user would paste it.
    let feed_id = writer.feed_id();
    let store_b = Corestore::new_mem().await;
    let reader = Peer::reader_with_swarm(
        &store_b,
        decode_public_key(&feed_id)?,
        Swarm::new(config()).await?,
        DiscoveryOptions::default().with_lookup_interval(Duration::from_secs(3)),
    )
    .await?;

    // Reaching here at all means the bootstrap entry arrived over a discovered connection.
    assert!(!reader.is_writer());
    assert_eq!(reader.public_key(), writer.public_key(), "feed key");
    assert_eq!(
        reader.public_keys().blobs,
        writer.public_keys().blobs,
        "blobs key"
    );
    assert_eq!(reader.topic(), writer.topic(), "both derive one topic");

    // The feed and blob cores were opened after the connection was established, and attach
    // to it automatically.
    until("the feed item to replicate", async || {
        Ok(reader.feed().has_key(b"hello").await?)
    })
    .await?;
    until("the blob to replicate", async || {
        Ok(reader.keyed_blobs().get(b"picture.png").await?.is_some())
    })
    .await?;
    assert_eq!(
        reader.keyed_blobs().get(b"picture.png").await?.as_deref(),
        Some(&b"not really a png"[..])
    );

    // Teardown leaves the topic and aborts the swarm tasks without panicking.
    drop(reader);
    Ok(())
}

/// The negative control for the test above, and the only coverage of the timeout path.
///
/// A reader pointed at a *different* bootstrap node cannot reach the writer, so it must give
/// up cleanly with [`PeerError::DiscoveryTimeout`] rather than hanging forever, which is what
/// the stream-based `Peer::reader` would do. `peers: 0` distinguishes "never found anyone"
/// from "connected but got no data".
#[tokio::test(flavor = "multi_thread")]
async fn a_reader_on_a_different_dht_times_out_instead_of_hanging() -> Result<()> {
    let writers_dht = bootstrap_node().await?;
    let unrelated_dht = bootstrap_node().await?;

    let store_a = Corestore::new_mem().await;
    let writer = Peer::writer_with_swarm(
        &store_a,
        "feed",
        Swarm::new(DhtConfig::default().add_bootstrap_node(writers_dht)).await?,
        DiscoveryOptions::default(),
    )
    .await?;

    let store_b = Corestore::new_mem().await;
    let result = Peer::reader_with_swarm(
        &store_b,
        decode_public_key(&writer.feed_id())?,
        Swarm::new(DhtConfig::default().add_bootstrap_node(unrelated_dht)).await?,
        DiscoveryOptions::default().with_timeout(Some(Duration::from_secs(2))),
    )
    .await;

    match result {
        Err(hyperrss::PeerError::DiscoveryTimeout { peers, .. }) => {
            assert_eq!(peers, 0, "no peer should have been reachable");
            Ok(())
        }
        Err(e) => panic!("expected DiscoveryTimeout, got {e}"),
        Ok(_) => panic!("reader somehow found a writer on an unrelated DHT"),
    }
}
