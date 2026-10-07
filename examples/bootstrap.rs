//! Run a standalone DHT bootstrap node, so `publish` and `subscribe` can be tried without
//! the public network.
//!
//! ```text
//! cargo run --example bootstrap
//! # then, in two more terminals, pass the printed address:
//! cargo run --example publish   -- ./store-a myfeed --bootstrap 127.0.0.1:PORT
//! cargo run --example subscribe -- ./store-b <feed-id> --bootstrap 127.0.0.1:PORT
//! ```

use std::error::Error;

use futures_lite::StreamExt;
use hyperdht::adht::Dht;
use hyperrss::DhtConfig;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:0".to_string());

    // `empty_bootstrap_nodes` means "join no network": this node *is* the network.
    let mut dht = Dht::with_config(
        DhtConfig::default()
            .empty_bootstrap_nodes()
            .set_address(addr.parse()?),
    )
    .await?;

    println!("bootstrap node listening on {}", dht.local_addr()?);
    println!("pass it with --bootstrap to the publish and subscribe examples");

    while dht.next().await.is_some() {}
    Ok(())
}
