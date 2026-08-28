# xbbg-core

Safe, zero-allocation Rust wrappers over the Bloomberg C++ SDK (`blpapi`).

This is the core abstraction layer between raw FFI (`xbbg-blpapi-sys`) and the async engine (`xbbg-async`). Every unsafe FFI call is wrapped in a safe Rust API with proper ownership, lifetimes, and error handling.

## Crate structure

```
src/
├── lib.rs           Module declarations + public re-exports
├── ffi.rs           Centralized FFI re-exports from blpapi_sys + local datetime types
│
├── Core types
│   ├── element.rs   Element wrapper — typed field access, iteration
│   ├── message.rs   Message wrapper — correlation IDs, topic name
│   ├── event.rs     Event + MessageIterator
│   ├── name.rs      Name interning with FxHashMap cache
│   ├── value.rs     Dynamic Value enum (replaces JSON serialization)
│   ├── datatype.rs  DataType enum mapping Bloomberg type codes
│   └── datetime.rs  HighPrecisionDatetime ↔ Arrow timestamp conversion
│
├── Session API
│   ├── session.rs      Session lifecycle (create, start, stop, events)
│   ├── service.rs      Service wrapper (open, create request, schema)
│   ├── request.rs      Request builder + schema validation
│   ├── options.rs      SessionOptions (connection, tuning, keep-alive)
│   ├── subscription.rs SubscriptionList for real-time data
│   ├── correlation.rs  CorrelationId (Int or Pointer variants)
│   ├── identity.rs     Identity handle for authenticated sessions
│   └── errors.rs       BlpError enum with rich context
│
└── schema/
    ├── mod.rs          SchemaStatus enum + module re-exports
    ├── operation.rs    Operation introspection
    ├── element_def.rs  SchemaElementDefinition
    ├── type_def.rs     SchemaTypeDefinition
    └── constant.rs     Constant + ConstantList for enumerations
```

## Features

| Feature | Description |
|---------|-------------|
| `live` (default) | Real Bloomberg SDK via `blpapi-sys` |

## Design principles

- **All FFI goes through `ffi.rs`** — single point of control for unsafe imports
- **Zero-allocation hot paths** — direct typed access, no JSON serialization
- **Sub-microsecond field extraction** — SIMD-accelerated where available
- **Owned vs borrowed** — `Value<'a>` borrows from Element; `OwnedValue` for storage

## Benchmarks

All benchmarks have been consolidated into the `xbbg-bench` crate. See `crates/xbbg-bench/README.md`.
