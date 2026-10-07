//! Finding peers over the DHT, so a feed can be shared with nothing but a key.
//!
//! Without this, a [`Peer`] can only replicate over a connection someone hands it. Here it
//! joins a [`hyperswarm`] topic derived from its `keys` core and replicates over every
//! connection the swarm produces, which is JS's `swarm.js`:
//!
//! ```js
//! const swarm = new Hyperswarm();
//! swarm.join(discoveryKey);
//! swarm.on('connection', conn => store.replicate(conn));
//! ```
//!
//! Both sides join as [`JoinOpts::Both`], matching JS's bare `swarm.join(discoveryKey)`. A
//! reader therefore also announces, which is the property the whole project rests on: every
//! reader re-shares the feed, so an audience makes a feed harder to suppress rather than
//! easier.
//!
//! It is worth knowing why that works, because the code reads as though it should not.
//! `PeerDiscovery::create_query` is `if opts.server() { Announce } else { Lookup }`, so a
//! `Both` peer only ever issues an announce and never a lookup. Peers are still found,
//! because an announce *is* a DHT query against the topic's closest nodes: `ActiveQuery` is a
//! `Stream<Item = Result<Option<LookupResponse>>>` and both of its variants yield the
//! responses that `PeerDiscovery` turns into peers. Verified by test, not just by reading.
//!
//! One deviation from the port target: **the reader re-issues its query on an interval**
//! ([`DiscoveryOptions::lookup_interval`]). A `PeerDiscovery` refreshes itself only every 10
//! to 12 minutes (`REFRESH_BASE_SECS` = 600, `REFRESH_JITTER_SECS` = 120), so a reader whose
//! first round completed before the writer announced would otherwise wait out that interval.

use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use corestore::Corestore;
use futures_lite::StreamExt;
use hypercore::VerifyingKey;
use hyperswarm::{JoinOpts, Swarm, Topic};
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use crate::peer::{Peer, PeerError, await_keys_entry, encode_public_key, topic_for_keys_core};

/// How a [`Peer`] behaves while it is joining the DHT.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct DiscoveryOptions {
    /// Budget for the constructor's network work: the first announce for a writer, and
    /// finding a peer plus reading its bootstrap entry for a reader. `None` waits forever,
    /// matching JS's plain `await`.
    ///
    /// Defaults to 30 seconds.
    pub timeout: Option<Duration>,
    /// How often a reader re-issues its topic lookup while waiting. See the [module
    /// docs](self) for why waiting on the swarm's own refresh is not viable.
    ///
    /// Defaults to 15 seconds. Ignored by writers.
    pub lookup_interval: Duration,
}

impl Default for DiscoveryOptions {
    fn default() -> Self {
        Self {
            timeout: Some(Duration::from_secs(30)),
            lookup_interval: Duration::from_secs(15),
        }
    }
}

impl DiscoveryOptions {
    /// Set [`DiscoveryOptions::timeout`].
    pub fn with_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.timeout = timeout;
        self
    }

    /// Set [`DiscoveryOptions::lookup_interval`].
    pub fn with_lookup_interval(mut self, interval: Duration) -> Self {
        self.lookup_interval = interval;
        self
    }
}

/// A peer's participation in the DHT: the swarm, the task driving it, the task turning swarm
/// connections into [`Corestore::replicate`] futures, and those futures' handles.
///
/// Dropping this leaves the topic and aborts every task it started.
pub(crate) struct Discovery {
    swarm: Swarm,
    topic: Topic,
    driver: JoinHandle<()>,
    connections: JoinHandle<()>,
    replications: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl std::fmt::Debug for Discovery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Discovery")
            .field("topic", &self.topic)
            .field("swarm", &self.swarm)
            .finish_non_exhaustive()
    }
}

impl Drop for Discovery {
    fn drop(&mut self) {
        // `leave` is synchronous (it takes a std `RwLock`, with nothing to await), so it is
        // safe here. It stops further announces and lookups but does *not* withdraw an
        // existing announcement: `Swarm::leave` carries a `TODO: Unannounce if was server`.
        self.swarm.leave(&self.topic);
        self.driver.abort();
        self.connections.abort();
        for handle in lock(&self.replications).drain(..) {
            handle.abort();
        }
    }
}

/// Take the replication-handle lock, ignoring poisoning: a panicked task leaves the `Vec`
/// perfectly usable, and the alternative is a panic in `Drop`.
fn lock(handles: &Mutex<Vec<JoinHandle<()>>>) -> std::sync::MutexGuard<'_, Vec<JoinHandle<()>>> {
    handles.lock().unwrap_or_else(|e| e.into_inner())
}

impl Discovery {
    /// Re-issue the topic query. `join` replaces the existing entry with a fresh
    /// `PeerDiscovery`, which is the only way to force one from outside `hyperswarm`:
    /// `PeerDiscovery::refresh` is public, but the map holding them is not.
    fn refresh(&self, opts: JoinOpts) {
        debug!(topic = ?self.topic, "re-issuing topic query");
        self.swarm.join(self.topic, opts);
    }
}

/// Start driving `swarm`, replicating `store` over everything it connects to, and join
/// `topic`.
fn start_discovery(store: &Corestore, swarm: Swarm, topic: Topic, join: JoinOpts) -> Discovery {
    // The driver goes first, deliberately. `SwarmInner` latches the first waker it is ever
    // polled with and never replaces it, so whichever task polls first receives every later
    // `maybe_wake`. If a short-lived `flush()` future won that race and was then dropped,
    // the wakes would go to a finished task.
    let mut driving = swarm.clone();
    let driver = tokio::spawn(async move { while driving.next().await.is_some() {} });

    // Subscribe before joining, so no connection event is missed.
    let mut conns = swarm.connections();
    let replications: Arc<Mutex<Vec<JoinHandle<()>>>> = Default::default();

    let loop_store = store.clone();
    let loop_handles = replications.clone();
    let connections = tokio::spawn(async move {
        while let Some(event) = conns.next().await {
            let event = match event {
                Ok(event) => event,
                Err(e) => {
                    warn!(error = %e, "swarm connection failed");
                    continue;
                }
            };
            debug!(
                remote = ?event.remote_public_key,
                client = event.client,
                "replicating corestore over a new swarm connection"
            );
            let replicating = loop_store.replicate(event.connection);
            // TODO(spawn): tracked in ../../TODO.md alongside the other `add_stream` spawn
            // sites. Unlike those, this one keeps the handle so `Drop` can abort it, and
            // logs the error instead of discarding it.
            let handle = tokio::spawn(async move {
                if let Err(e) = replicating.await {
                    warn!(error = %e, "replication over a swarm connection ended in error");
                }
            });
            let mut handles = lock(&loop_handles);
            handles.retain(|h| !h.is_finished());
            handles.push(handle);
        }
    });

    swarm.join(topic, join);
    Discovery {
        swarm,
        topic,
        driver,
        connections,
        replications,
    }
}

impl Peer {
    /// The feed address to share: `base64url` of the `keys` core's public key. This is the
    /// string [`Peer::reader_with_swarm`] takes, via [`decode_public_key`](crate::decode_public_key)
    /// (JS: `writer.js`'s `discoveryKeyString()`).
    pub fn feed_id(&self) -> String {
        encode_public_key(&self.keys_core_public_key())
    }

    /// The DHT topic this feed is announced under, whether or not this peer has a swarm.
    pub fn topic(&self) -> Topic {
        topic_for_keys_core(&self.keys_core_public_key())
    }

    /// The swarm this peer discovers over, if it was built with one.
    ///
    /// Do not call [`Swarm::connections`] on it. A swarm has exactly one connection receiver
    /// and this peer already owns it, so a second stream would steal its events.
    pub fn swarm(&self) -> Option<&Swarm> {
        self.discovery.as_ref().map(|d| &d.swarm)
    }

    /// Leave the topic and tear down this peer's swarm tasks and connections. Idempotent, and
    /// done automatically on drop.
    ///
    /// This stops further announces, lookups and replication, but does not withdraw an
    /// announcement already on the DHT, so other peers may keep dialling this address until
    /// it expires.
    pub fn close(&mut self) {
        self.discovery = None;
    }

    /// A writer that announces itself on the DHT, so that readers holding its
    /// [`Peer::feed_id`] can find it.
    ///
    /// Takes ownership of `swarm`: this peer joins [`Peer::topic`] as a server, owns the
    /// swarm's connection stream, and replicates `store` over every connection that arrives.
    /// Returns once the first announce round has flushed (JS: `await peerDiscovery.flushed()`).
    ///
    /// Does not bootstrap the DHT. Against the public network, call [`Swarm::bootstrap`]
    /// before passing the swarm in.
    pub async fn writer_with_swarm(
        store: &Corestore,
        name: &str,
        swarm: Swarm,
        opts: DiscoveryOptions,
    ) -> Result<Self, PeerError> {
        let mut peer = Self::writer(store, name).await?;
        let topic = peer.topic();
        let discovery = start_discovery(store, swarm, topic, JoinOpts::Both);

        match opts.timeout {
            Some(t) => tokio::time::timeout(t, discovery.swarm.flush())
                .await
                .map_err(|_| PeerError::AnnounceTimeout(t))??,
            None => discovery.swarm.flush().await?,
        }

        peer.discovery = Some(discovery);
        Ok(peer)
    }

    /// Bootstrap a reader from just the `keys` core's public key, finding the writer over the
    /// DHT rather than over a stream you supply (compare [`Peer::reader`]).
    ///
    /// Takes ownership of `swarm`: joins [`Peer::topic`] as a client, replicates `store` over
    /// every connection the swarm produces, then waits for the writer's bootstrap entry,
    /// bounded by [`DiscoveryOptions::timeout`] and re-issuing the lookup every
    /// [`DiscoveryOptions::lookup_interval`] (JS: `reader.js`'s `keysCore.get(0)`, guarded
    /// there by `store.findingPeers()`, which has no Rust equivalent).
    ///
    /// Does not bootstrap the DHT. Against the public network, call [`Swarm::bootstrap`]
    /// before passing the swarm in.
    pub async fn reader_with_swarm(
        store: &Corestore,
        keys_core_public_key: VerifyingKey,
        swarm: Swarm,
        opts: DiscoveryOptions,
    ) -> Result<Self, PeerError> {
        let keys_core = store.get_from_verifying_key(&keys_core_public_key).await?;
        let topic = topic_for_keys_core(&keys_core_public_key);
        let discovery = start_discovery(store, swarm, topic, JoinOpts::Both);

        let deadline = opts.timeout.map(|t| Instant::now() + t);
        let keys = await_keys_entry(&keys_core, deadline, opts.lookup_interval, || {
            discovery.refresh(JoinOpts::Both)
        })
        .await?;

        let mut peer = Self::from_keys(store, keys_core, keys).await?;
        peer.discovery = Some(discovery);
        Ok(peer)
    }
}
