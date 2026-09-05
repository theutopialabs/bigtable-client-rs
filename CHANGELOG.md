# Changelog

All notable changes to this project are documented in this file.

## 0.0.1

Initial release of the Bigtable data client and its `FromRow` derive crate.

- High-level row reads, row mutations, and bounded bulk writes.
- Retry, deadline, request-diagnostics, tracing, and OpenTelemetry support.
- Typed row mapping with `FromRow` and the `bigtable-client-derive` macro.
- Application default credentials, channel pooling, and Bigtable emulator tests.
