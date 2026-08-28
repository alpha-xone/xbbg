# xbbg-recipes

High-level recipe functions built on `xbbg-async` and `xbbg-ext`.

This crate groups reusable market-data workflows such as:

- fixed-income analytics helpers
- futures helper workflows
- historical data transformations
- common error types for recipe-level APIs

The crate is independent and uses the shared xbbg Rust engine for BLPAPI-backed request execution when the `live` feature is enabled. Runtime use requires separately licensed service access and local BLPAPI runtime components where applicable.
