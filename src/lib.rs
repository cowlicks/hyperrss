/*!
TODO Crate level docs
```rust
# use rust::add;
assert_eq!(add(1, 2), 3);
```
*/
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

use tracing::info;

/// This is documented
#[tracing::instrument]
pub fn add(left: usize, right: usize) -> usize {
    info!("logging is good");
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
