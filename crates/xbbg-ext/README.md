# xbbg-ext

Extension utilities shared by xbbg language bindings and higher-level crates.

This crate contains pure Rust helpers for:

- futures and CDX ticker resolution
- market-session and timezone derivation
- currency and historical-data transformations
- fixed-income and BQL-oriented transformation helpers

The crate is independent and is designed to sit above the low-level BLPAPI-backed runtime crates. Some helpers operate on data returned by separately licensed services, but this crate does not provide service access by itself.
