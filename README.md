# Usage

## Examples

A feed is addressed by one string, the `base64url` public key of its `keys` core. Give that
to a subscriber and it finds the publisher over the DHT.

Against the public DHT:

```sh
cargo run --example publish   -- ./store-a myfeed    # prints the feed id, then keeps appending
cargo run --example subscribe -- ./store-b <feed-id> # prints items as they arrive
```

The stores are on disk, so restarting `publish` against the same directory keeps the same
feed id.

To try it without the public network, run your own bootstrap node and point both at it:

```sh
cargo run --example bootstrap                        # prints 127.0.0.1:PORT
cargo run --example publish   -- ./store-a myfeed    --bootstrap 127.0.0.1:PORT
cargo run --example subscribe -- ./store-b <feed-id> --bootstrap 127.0.0.1:PORT
```

Set `RUST_LOG=hyperrss=debug,hyperswarm=debug` to watch discovery happen.

Two peers that are both behind NAT will not connect yet: holepunching is unimplemented in
`hyperdht`. Same-machine, same-LAN, and one-public-peer all work.

## formatting

To format source code, `rustfmt` needs to run with nightly ([more info](https://github.com/rust-lang/rustfmt?tab=readme-ov-file#on-the-nightly-toolchain)): `cargo +nightly fmt`.

## Releasing

* Releasing run `cargo release  ... `
