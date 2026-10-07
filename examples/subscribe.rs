//! Subscribe to a feed over the public DHT, given only its feed id.
//!
//! ```text
//! cargo run --example subscribe -- ./store-b <feed-id>
//! ```
//!
//! Set `RUST_LOG=hyperrss=debug,hyperswarm=debug` to watch discovery happen.

use std::{error::Error, time::Duration};

use corestore::Corestore;
use futures_lite::StreamExt;
use hyperrss::{DhtConfig, DiscoveryOptions, Peer, PeerError, Swarm, decode_public_key};

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
    let (Some(dir), Some(feed_id)) = (args.next(), args.next()) else {
        eprintln!("usage: subscribe <store-dir> <feed-id> [--bootstrap <addr>]");
        std::process::exit(2);
    };
    let keys_core_public_key = decode_public_key(&feed_id)?;

    let store = Corestore::new_disk(&dir).await?;
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

    println!("looking for peers publishing {feed_id}...");
    let peer = match Peer::reader_with_swarm(
        &store,
        keys_core_public_key,
        swarm,
        DiscoveryOptions::default().with_timeout(Some(Duration::from_secs(60))),
    )
    .await
    {
        Ok(peer) => peer,
        // The error already says whether the problem was reaching anybody at all or getting
        // data out of whoever answered, so just surface it.
        Err(e @ PeerError::DiscoveryTimeout { .. }) => {
            eprintln!("\n{e}");
            eprintln!(
                "\nIs the publisher still running? Two peers behind NAT cannot connect yet: \
                 holepunching is unimplemented in hyperdht."
            );
            std::process::exit(1);
        }
        Err(e) => return Err(e.into()),
    };

    println!(
        "connected. feed key: {}\n",
        hyperrss::encode_public_key(&peer.public_key())
    );

    let mut seen = 0usize;
    loop {
        // Newest first, so take everything and re-print only what is new.
        let mut entries = Vec::new();
        if let Ok(stream) = peer.feed().get_feed_stream().await {
            let mut stream = Box::pin(stream);
            // Mid-replication reads fail rather than reporting absence, so stop at the first
            // error and let the next pass pick the rest up.
            while let Some(Ok(entry)) = stream.next().await {
                entries.push(entry);
            }
        }

        if entries.len() > seen {
            for entry in entries.iter().take(entries.len() - seen).rev() {
                println!(
                    "{}: {}",
                    String::from_utf8_lossy(&entry.key),
                    entry
                        .value
                        .as_deref()
                        .map(String::from_utf8_lossy)
                        .unwrap_or_default()
                );
            }
            seen = entries.len();
        }

        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
        }
    }

    println!("\nshutting down");
    Ok(())
}
