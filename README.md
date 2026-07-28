# Bigtable client for Rust

An async, production-focused Rust client for Google Cloud Bigtable.

This project is under active development. M0 provides the connection layer,
application default credentials, emulator support, standard Bigtable metadata,
and direct access to the generated Tonic client. The first supported version
will be `0.0.1` after M5.

This crate is not published to crates.io.

## Current support

| Capability | Status | Milestone |
| --- | --- | --- |
| Native Tonic gRPC | Available | M0 |
| Application default credentials | Available | M0 |
| Channel pooling and deadlines | Available | M0 |
| Emulator integration tests | Available | M0 |
| High-level row query API | Planned | M1 |
| Retry and deadline policies | Planned | M1 |
| Bulk mutations with partial retries | Planned | M2 |
| Typed row mapping | Planned | M3 |
| Tracing and OpenTelemetry | Planned | M4 |
| Production hardening and `0.0.1` | Planned | M5 |

The client currently covers the Bigtable data API. Instance, cluster, and table
administration are outside the public API.

## Installation

Until `0.0.1` is ready, depend on the Git repository:

```toml
[dependencies]
bigtable-client = { git = "https://github.com/theutopialabs/bigtable-client-rs", branch = "main" }
```

Pin a commit with `rev` when you need reproducible builds.

The minimum supported Rust version is 1.88.

## Quick start

M0 exposes the generated Tonic client for direct requests:

```rust,no_run
use bigtable_client::{Client, ClientConfig, proto::PingAndWarmRequest};
use tonic::Request;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let config = ClientConfig::new("my-project", "my-instance")?;
let client = Client::connect(config).await?;
let mut raw = client.raw_client();

let mut request = Request::new(PingAndWarmRequest {
    name: "projects/my-project/instances/my-instance".to_owned(),
    app_profile_id: "default".to_owned(),
});
request.metadata_mut().insert(
    "x-goog-request-params",
    "name=projects/my-project/instances/my-instance".parse()?,
);

raw.ping_and_warm(request).await?;
# Ok(())
# }
```

`Client` adds authorization, `x-goog-api-client`, and `bigtable-features`
metadata. Raw callers must add the `x-goog-request-params` routing header for
each RPC. M1 will add high-level request builders that do this automatically.

Clone `Client` or `RawClient` instead of creating a client per request. Clones
share channels and token refresh state.

## Authentication

Production connections use Google application default credentials. The client
checks the credential sources supported by `gcp_auth`, including:

- `GOOGLE_APPLICATION_CREDENTIALS`
- local application default credentials from `gcloud auth application-default login`
- the Google Cloud metadata server
- the active `gcloud` account

You can also pass an `Arc<dyn gcp_auth::TokenProvider>` to
`Client::connect_with_token_provider`.

The default scope is `https://www.googleapis.com/auth/bigtable.data`.

## Configuration

Create a config in code:

```rust
use std::time::Duration;

use bigtable_client::ClientConfig;

# fn config() -> Result<ClientConfig, bigtable_client::Error> {
ClientConfig::new("my-project", "my-instance")?
    .with_app_profile_id("default")?
    .with_channel_pool_size(1)?
    .with_connect_timeout(Duration::from_secs(10))?
    .with_request_timeout(Duration::from_secs(60))
# }
```

Or call `ClientConfig::load()` to read these environment variables:

| Variable | Required | Default |
| --- | --- | --- |
| `BIGTABLE_PROJECT_ID` | Yes | None |
| `BIGTABLE_INSTANCE_ID` | Yes | None |
| `BIGTABLE_APP_PROFILE_ID` | No | `default` |
| `BIGTABLE_ENDPOINT` | No | `https://bigtable.googleapis.com` |
| `BIGTABLE_EMULATOR_HOST` | No | None |
| `BIGTABLE_CHANNEL_POOL_SIZE` | No | `1` |
| `BIGTABLE_CONNECT_TIMEOUT` | No | `10s` |
| `BIGTABLE_REQUEST_TIMEOUT` | No | `60s` |
| `BIGTABLE_KEEP_ALIVE_INTERVAL` | No | `60s` |
| `BIGTABLE_KEEP_ALIVE_TIMEOUT` | No | `20s` |

Durations accept values such as `500ms`, `10s`, and `2m`.

## Emulator

Start the official Bigtable emulator:

```bash
docker run --rm -p 127.0.0.1:8086:8086 \
  gcr.io/google.com/cloudsdktool/google-cloud-cli:574.0.0-emulators \
  gcloud beta emulators bigtable start --host-port=0.0.0.0:8086
```

Then configure the client:

```bash
export BIGTABLE_PROJECT_ID=test-project
export BIGTABLE_INSTANCE_ID=test-instance
export BIGTABLE_EMULATOR_HOST=127.0.0.1:8086
```

Emulator connections use plaintext and skip authentication. The emulator is
for local tests only.

Run the end-to-end test with:

```bash
RUN_BIGTABLE_EMULATOR_TESTS=1 \
BIGTABLE_EMULATOR_HOST=127.0.0.1:8086 \
cargo test --features emulator-tests --test emulator
```

See the
[Google Bigtable emulator guide](https://cloud.google.com/bigtable/docs/emulator)
for emulator behavior and limits.

## Development

Run the same checks used in CI:

```bash
cargo fmt --all --check
cargo check --workspace --all-targets --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
```

Every milestone must pass unit, public API, documentation, MSRV, and emulator
tests before it is merged.

## Roadmap

- M0: workspace, configuration, auth, channels, raw Tonic client, emulator CI
- M1: row model, query builders, stream assembly, retry and deadline policies
- M2: single-row and bulk mutations, flow control, partial retry handling
- M3: typed row mapping and derive support
- M4: tracing spans, OpenTelemetry metrics, request diagnostics
- M5: compatibility review, stress tests, docs, and version `0.0.1`

No milestone will be published to crates.io.

## License

Licensed under the
[MIT License](https://github.com/theutopialabs/bigtable-client-rs/blob/main/LICENSE).
