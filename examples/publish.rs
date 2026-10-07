//! Publish a feed to the public DHT and keep appending to it.
//!
//! ```text
//! cargo run --example publish -- ./store-a myfeed
//! ```
//!
//! Prints the feed id to hand to `subscribe`. The store is on disk, so stopping and
//! restarting against the same directory keeps the same feed id.
//!
//! Set `RUST_LOG=hyperrss=debug,hyperswarm=debug` to watch discovery happen.

use std::{
    error::Error,
    time::{Duration, SystemTime},
};

use corestore::Corestore;
use hyperrss::{DhtConfig, DiscoveryOptions, Peer, Swarm};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let argv: Vec<String> = std::env::args().skip(1).collect();
    // A local bootstrap node (see the `bootstrap` example) instead of the public DHT.
    let bootstrap = argv
        .iter()
        .position(|a| a == "--bootstrap")
        .and_then(|i| argv.get(i + 1))
        .map(|a| a.parse::<std::net::SocketAddr>())
        .transpose()?;
    let mut positional = Vec::new();
    let mut rest = argv.iter();
    while let Some(a) = rest.next() {
        if a == "--bootstrap" {
            rest.next();
        } else if !a.starts_with("--") {
            positional.push(a.clone());
        }
    }
    let mut args = positional.into_iter();
    let (Some(dir), Some(name)) = (args.next(), args.next()) else {
        eprintln!("usage: publish <store-dir> <feed-name> [--bootstrap <addr>]");
        std::process::exit(2);
    };

    let store = Corestore::new_disk(&dir).await?;

    // The constructors deliberately do not bootstrap, so that tests can run against an
    // isolated DHT. Reaching the public network is this caller's choice.
    let config = match bootstrap {
        Some(addr) => DhtConfig::default().add_bootstrap_node(addr),
        None => DhtConfig::default(),
    };
    let swarm = Swarm::new(config).await?;
    match bootstrap {
        Some(addr) => println!("bootstrapping against {addr}..."),
        None => println!("bootstrapping against the public DHT..."),
    }
    swarm.bootstrap().await?;

    let peer = Peer::writer_with_swarm(&store, &name, swarm, DiscoveryOptions::default()).await?;

    println!("\n  feed id (share this):\n\n      {}\n", peer.feed_id());
    println!(
        "  subscribe with:\n\n      cargo run --example subscribe -- ./store-b {}\n",
        peer.feed_id()
    );

    // Append on a timer so a subscriber that attaches later sees the feed grow live.
    let mut n = peer.feed().version().await;
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            _ = tokio::time::sleep(Duration::from_secs(5)) => {
                let now = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)?
                    .as_secs();
                let key = format!("item-{n}");
                peer.feed()
                    .put_ordered_item(key.as_bytes(), format!("published at {now}").as_bytes())
                    .await?;
                println!("appended {key}");
                n += 1;
            }
        }
    }

    println!("\nleaving the topic and shutting down");
    drop(peer);
    Ok(())
}
