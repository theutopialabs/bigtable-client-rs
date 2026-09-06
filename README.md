# Bigtable client for Rust

An async Rust client for Google Cloud Bigtable.

Version `0.0.1` provides high-level reads and writes, typed row mapping, safe
partial retries, bounded bulk requests, deadline policies, tracing,
OpenTelemetry metrics, request diagnostics, and direct access to the generated
Tonic client.

The workspace is prepared for crates.io publication. A source checkout alone
does not establish that any version is available in the registry.

## Installation

Until a release is available in crates.io, depend on the Git repository:

```toml
[dependencies]
bigtable-client = { git = "https://github.com/theutopialabs/bigtable-client-rs", branch = "main" }
```

Pin a commit with `rev` when you need reproducible builds.

The current source version is `0.0.1`. When that version has been published,
use the registry package instead:

```toml
[dependencies]
bigtable-client = "0.0.1"
```

## Publishing a release

Pushing an exact version tag such as `v0.0.1` starts the publish workflow. The
workflow accepts only tags whose commit is reachable from `main` and whose
version exactly matches both workspace crates. A new release must use the current
`main` commit. It requires the repository's Actions secret `CARGO_REGISTRY_TOKEN`
with permission to publish the packages.

The workflow runs the release checks and live Bigtable emulator tests on the
tagged commit, publishes `bigtable-client-derive`, waits for that exact version
to be indexed by crates.io, and then publishes `bigtable-client`. It rechecks
`main` immediately before publishing the derive crate. Once that exact derive
version is published, the workflow can finish or resume the release if `main`
advances. Reruns skip crate versions already published and repeat validation on
the tagged commit. Create release tags only from `main`; the workflow does not
create a GitHub Release.

## Compatibility and support

The minimum supported Rust version is 1.88. CI checks the default feature set,
`default-features = false`, release builds, rustdoc, packaged crates, and the
official Bigtable emulator.

Version `0.0.1` is the first release candidate. The API can still change
between `0.0.x` versions. Pin a Git revision when an application needs a stable
source build; crates.io availability remains contingent on an actual release.

The public API covers the Bigtable data service. Use the generated Tonic client
for data RPCs that do not have a high-level wrapper. Instance, cluster, table,
backup, and IAM administration are not supported.

## Quick start

```rust,no_run
use bigtable_client::{Client, ClientConfig, Query};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let config = ClientConfig::new("my-project", "my-instance")?;
let client = Client::connect(config).await?;
let query = Query::new("events")?
    .prefix(b"user#".to_vec())
    .limit(100)?;
let mut rows = client.read_rows(query).await?;

while let Some(row) = rows.next().await {
    let row = row?;
    println!("row key: {:?}", row.key);
}
# Ok(())
# }
```

The high-level client adds authorization, routing, `x-goog-api-client`,
`bigtable-features`, and a per-attempt gRPC deadline.

Clone `Client` or `RawClient` instead of creating a client per request. Clones
share channels and token refresh state.

Create one client per application process in most cases. Start with one channel
and increase `channel_pool_size` only when observed concurrency needs it. Use a
separate application profile for workloads that need different routing or
isolation.

## Row queries

`Query` follows the shape of the Google client libraries. A query can combine
exact keys, ranges, prefixes, a proto row filter, a row limit, and reverse
ordering.

```rust,no_run
use bigtable_client::{Client, Query, RowBound, RowRange, proto::RowFilter};

# async fn read(client: &Client) -> Result<(), bigtable_client::Error> {
let query = Query::new("events")?
    .row_key(b"event#100".to_vec())
    .row_range(RowRange::new(
        RowBound::inclusive(b"event#200".to_vec()),
        RowBound::exclusive(b"event#300".to_vec()),
    ))
    .filter(RowFilter::default())
    .limit(50)?
    .reversed();

let mut rows = client.read_rows(query).await?;
while let Some(row) = rows.next().await {
    let row = row?;
    println!("{:?}", row);
}
# Ok(())
# }
```

Use `Client::read_row` for one exact key. It returns `None` when the row does
not exist.

Rows own their data and keep raw row keys, qualifiers, and values as
`bytes::Bytes`. The model is:

```text
Row
└── Family
    └── Column
        └── Cell
```

Families, columns, and cells stay in server response order. Cell timestamps are
in microseconds. Filter labels are kept on each cell. The client accepts gRPC
response messages up to 256 MiB.

## Retries and deadlines

Read retries are enabled by default:

| Setting | Default |
| --- | --- |
| Retryable gRPC codes | `Cancelled`, `DeadlineExceeded`, `Unavailable`, `Aborted` |
| Maximum attempts | `10` |
| Initial backoff | `10ms` |
| Backoff multiplier | `2` |
| Maximum backoff | `60s` |
| Jitter | Full |
| Attempt timeout | `20s` |
| Operation timeout | `BIGTABLE_REQUEST_TIMEOUT`, default `10m` |

The client honors Google `RetryInfo` delays. On a broken stream it drops only
the incomplete row, keeps rows already returned, and resumes after the latest
committed row or `last_scanned_row_key`. Point keys and row ranges are trimmed
for both forward and reverse scans. A row limit is reduced by the number of
rows already returned.

Use `ReadOptions` when one operation needs different settings:

```rust,no_run
use std::time::Duration;

use bigtable_client::{
    Client, DeadlinePolicy, Jitter, Query, ReadOptions, RetryPolicy,
};

# async fn read(client: &Client) -> Result<(), bigtable_client::Error> {
let options = ReadOptions {
    retry: RetryPolicy {
        max_attempts: 5,
        initial_backoff: Duration::from_millis(25),
        max_backoff: Duration::from_secs(5),
        multiplier: 2.0,
        jitter: Jitter::Full,
    },
    deadlines: DeadlinePolicy {
        operation_timeout: Duration::from_secs(120),
        attempt_timeout: Duration::from_secs(20),
    },
};

let query = Query::new("events")?.prefix(b"today#".to_vec());
let mut rows = client.read_rows_with_options(query, options).await?;
while let Some(row) = rows.next().await {
    println!("{:?}", row?.key);
}
# Ok(())
# }
```

Reads start before the returned `RowStream` is consumed and buffer up to 16
rows. Dropping the stream cancels the background read, including an idle RPC
or retry backoff. The operation deadline also applies while the application
is not consuming rows. Buffered rows remain available before the terminal
deadline error. If an attempt times out first, a retry resumes after the last
row successfully buffered.

## Typed rows

Derive `FromRow` when a Bigtable row has a stable application schema:

```rust,no_run
use bigtable_client::{Client, FromRow, Query};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Preferences {
    theme: String,
}

#[derive(Debug, FromRow)]
#[bigtable(family = "profile")]
struct User {
    #[bigtable(row_key)]
    key: String,
    name: String,
    #[bigtable(qualifier = "is_active")]
    active: bool,
    nickname: Option<String>,
    #[bigtable(default)]
    visits: u64,
    #[bigtable(json)]
    preferences: Preferences,
}

# async fn read(client: &Client) -> Result<(), bigtable_client::Error> {
let user = client
    .read_row_as::<User>("users", b"user#42".to_vec())
    .await?;

let query = Query::new("users")?.prefix(b"user#".to_vec());
let mut users = client.read_rows_as::<User>(query).await?;
while let Some(user) = users.next().await {
    println!("{:?}", user?);
}
# Ok(())
# }
```

A field uses its Rust name as the qualifier unless `qualifier` overrides it.
Set a default family on the struct or set `family` on an individual field.
Qualifiers accept string or byte string literals.

| Attribute | Behavior |
| --- | --- |
| `row_key` | Decode the row key into this field |
| `family = "name"` | Select a column family |
| `qualifier = "name"` | Select a UTF-8 qualifier |
| `qualifier = b"\xff"` | Select a binary qualifier |
| `json` | Decode the cell with Serde JSON |
| `with = "path"` | Call `fn(&[u8]) -> Result<T, E>` |
| `default` | Use `Default` when the column is absent |

Plain fields are required and use the latest cell visible after server-side
filters. `Option<T>` fields return `None` when the family, column, or cell is
absent. Unknown columns are ignored. Mapping one bad row returns an error for
that stream item and does not stop later rows.

`FromCellValue` supports `bytes::Bytes`, `Vec<u8>`, UTF-8 `String`, booleans,
integers, and floats. Boolean and number values use UTF-8 text. Use `with` or a
custom `FromCellValue` implementation when your schema uses protobuf, fixed
width numbers, or another binary encoding.

Use `RowDecoder` for manual mappings. It supports required and optional cells,
JSON, custom decoder functions, raw cells, and all visible versions.
`RowDecoder::versions` returns `DecodedCell<T>` values with timestamps and
filter labels in Bigtable's decreasing timestamp order.

Mapping failures use `Error::RowMapping`. The source keeps the raw row key and a
typed issue for a missing family, column, cell, or invalid value. Decode errors
also include the family, qualifier, timestamp, and target Rust type.

The typed API includes `read_row_as`, `read_rows_as`, and variants that accept
`ReadOptions`. Deriving a mapper does not change the query or add filters. Keep
queries narrow so every required field is returned.

## Mutations

Build one atomic row mutation from ordered cell and row changes:

```rust,no_run
use bigtable_client::{Client, Mutation, RowMutation};

# async fn write(client: &Client) -> Result<(), bigtable_client::Error> {
let row = RowMutation::new(b"user#42".to_vec())?
    .mutation(Mutation::set_cell(
        "profile",
        b"name".to_vec(),
        b"Ada".to_vec(),
    )?)?
    .mutation(Mutation::delete_cells(
        "profile",
        b"old_name".to_vec(),
    )?)?;

client.mutate_row("users", row).await?;
# Ok(())
# }
```

`Mutation` supports setting a cell, deleting all versions of a cell, deleting
a timestamp range, deleting a family, and deleting a row. Use
`Mutation::from_proto` for data API operations that do not have a builder yet.
Row keys, qualifiers, and values accept binary data.

Each `RowMutation` is atomic. Its changes run in order. Separate row entries
may run in any order, including entries for the same row.

Use `BulkMutation` to write many rows:

```rust,no_run
use bigtable_client::{BulkMutation, Client, Mutation, RowMutation};

# async fn write(client: &Client) -> Result<(), bigtable_client::Error> {
let first = RowMutation::new(b"user#1".to_vec())?
    .mutation(Mutation::set_cell("profile", b"name".to_vec(), b"Ada".to_vec())?)?;
let second = RowMutation::new(b"user#2".to_vec())?
    .mutation(Mutation::set_cell("profile", b"name".to_vec(), b"Lin".to_vec())?)?;
let bulk = BulkMutation::new("users")?
    .entry(first)?
    .entry(second)?;

let result = client.mutate_rows(bulk).await?;
println!(
    "{} rows in {} request batches",
    result.entries(),
    result.request_batches(),
);
# Ok(())
# }
```

Bulk mutation defaults are:

| Setting | Default |
| --- | --- |
| Retryable gRPC codes | `DeadlineExceeded`, `Unavailable` |
| Maximum attempts | `10` |
| Initial backoff | `10ms` |
| Backoff multiplier | `2` |
| Maximum backoff | `60s` |
| Jitter | Full |
| Attempt timeout | `60s` |
| Operation timeout | `10m` |
| Entries per request | `100` |
| Target encoded request size | `20 MiB` |
| Requests in flight | `5` |

`BulkMutationOptions` can change every value in this table. The request byte
limit is a target. One larger row entry is sent by itself. The client also
keeps each RPC below Bigtable's limit of 100,000 mutations.

`Mutation::set_cell` records client time at millisecond precision. Its fixed
timestamp makes a retry write the same cell version. Use
`Mutation::set_cell_at_server_time` only when a new server timestamp is
required. The client does not replay that entry after an ambiguous failure.
Advanced aggregate mutations are retry safe only when the row has a stable
idempotency token.

The client retries only unresolved entries that are safe to replay. Confirmed
entries are never sent again. If any entry still lacks a confirmed success,
`Error::BulkMutation` reports the original entry indexes, attempts, retry
safety, rich gRPC status, and partial success count:

```rust,no_run
use bigtable_client::{BulkMutation, Client, Error};

# async fn write(
#     client: &Client,
#     bulk: BulkMutation,
# ) -> Result<(), bigtable_client::Error> {
match client.mutate_rows(bulk).await {
    Ok(result) => println!("wrote {} rows", result.entries()),
    Err(Error::BulkMutation(error)) => {
        for failure in error.failures() {
            eprintln!(
                "entry {} failed (replay safe: {}): {}",
                failure.index(), failure.retry_safe(), failure.cause(),
            );
        }
    }
    Err(error) => return Err(error),
}
# Ok(())
# }
```

`MutationFailure::retry_safe` reports the entry's idempotency for every failure
cause, including deadlines and malformed responses. It does not mean the
underlying error is transient or that an automatic retry will succeed.

Dropping a bulk mutation future cancels active request streams and prevents new
batches from starting.

## Observability

High-level reads and bulk mutations create `tracing` spans for the full
operation and each RPC attempt:

```text
bigtable.client.operation
└── bigtable.client.attempt
```

The spans include the RPC method, Bigtable resource IDs, attempt and batch
counts, timeouts, final gRPC status, elapsed time, and row or mutation counts.
Retries emit a structured warning event with the failed attempt, gRPC code,
backoff delay, and unresolved entry count.

Install a tracing subscriber in the application to collect these spans. The
library does not install a subscriber or exporter.

OpenTelemetry metrics are enabled by the default `opentelemetry` feature. The
client gets a meter from the global provider when it connects:

```rust,no_run
use bigtable_client::{Client, ClientConfig};

# async fn run() -> Result<(), bigtable_client::Error> {
// Install your OpenTelemetry meter provider before this call.
let client = Client::builder(ClientConfig::new("my-project", "my-instance")?)
    .connect()
    .await?;
# let _ = client;
# Ok(())
# }
```

Use `ClientBuilder::with_meter` when the client should use a specific meter
instead. The application owns the meter provider, readers, exporters, flushing,
and shutdown.

| Metric | Meaning |
| --- | --- |
| `bigtable.googleapis.com/client/operation_latencies` | Full operation time across attempts and backoff |
| `bigtable.googleapis.com/client/attempt_latencies` | Time for one RPC attempt |
| `bigtable.googleapis.com/client/retry_count` | Extra RPC attempts |
| `bigtable.googleapis.com/client/first_response_latencies` | Time to the first streamed response |
| `bigtable.googleapis.com/client/application_blocking_latencies` | Time a read waits for the application |

Metric attributes use bounded resource and request fields: project, instance,
table, app profile, method, streaming mode, gRPC status, and client version.

Use a diagnostics observer when an application needs request lifecycle events
without parsing logs:

```rust,no_run
use bigtable_client::{Client, ClientConfig, DiagnosticEvent};

# async fn run() -> Result<(), bigtable_client::Error> {
let client = Client::builder(ClientConfig::new("my-project", "my-instance")?)
    .with_diagnostic_observer(|event: &DiagnosticEvent| {
        println!("{event:?}");
    })
    .connect()
    .await?;
# let _ = client;
# Ok(())
# }
```

Observers receive operation, attempt, first-response, retry, completion, and
cancellation events. They run inline and should return quickly.
`with_shared_diagnostic_observer` accepts an `Arc<dyn DiagnosticObserver>` for
reuse across clients. An observer panic is reported through `tracing` and does
not stop the request.

Spans, metrics, and diagnostics never include row keys, column qualifiers, cell
values, idempotency tokens, or gRPC messages. Raw Tonic calls are not
instrumented because their lifecycle belongs to the caller.

Disable metrics while keeping tracing and diagnostics with:

```toml
[dependencies]
bigtable-client = { git = "https://github.com/theutopialabs/bigtable-client-rs", default-features = false }
```

## Raw Tonic client

Use `Client::raw_client` for data API calls that do not have a high-level
wrapper yet:

```rust,no_run
use std::time::Duration;

use bigtable_client::{Client, ClientConfig, proto::PingAndWarmRequest};
use tonic::Request;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let client = Client::connect(ClientConfig::new("my-project", "my-instance")?).await?;
let mut raw = client.raw_client();
let name = "projects/my-project/instances/my-instance";
let mut request = Request::new(PingAndWarmRequest {
    name: name.to_owned(),
    app_profile_id: "default".to_owned(),
});
request.metadata_mut().insert(
    "x-goog-request-params",
    format!("name={name}").parse()?,
);
request.set_timeout(Duration::from_secs(20));
raw.ping_and_warm(request).await?;
# Ok(())
# }
```

Raw requests receive authentication and standard feature metadata. Raw callers
must add routing metadata and a deadline for each RPC.

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
| `BIGTABLE_REQUEST_TIMEOUT` | No | `10m` |
| `BIGTABLE_KEEP_ALIVE_INTERVAL` | No | `60s` |
| `BIGTABLE_KEEP_ALIVE_TIMEOUT` | No | `20s` |

Durations accept values such as `500ms`, `10s`, and `2m`.

Production endpoints must use HTTPS. Emulator endpoints skip authentication;
an omitted scheme defaults to HTTP, while an explicit HTTPS scheme enables TLS.

## Service limits

The high-level API rejects inputs that exceed these hard Bigtable limits:

| Input | Limit |
| --- | --- |
| Table ID | 50 characters |
| Application profile ID | 50 characters |
| Column family ID | 64 characters |
| Row key or read range bound | 4 KiB |
| Mutations in one request | 100,000 |

Exact read keys must not be empty. Empty range bounds stay valid and mean an
unbounded side of the range.

Bigtable also recommends keeping qualifiers at or below 16 KiB and cell values
at or below 10 MiB. The service hard limits are 100 MiB per cell, 256 MiB per
row, and 200 MiB per mutation. The client does not allocate or copy a large
value just to validate these size recommendations. Keep bulk requests near the
default 20 MiB target.

See Google's
[quotas and limits](https://cloud.google.com/bigtable/quotas) for the current
service contract.

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

The official emulator uses plaintext and skips authentication. It is for local
tests only.

Run the end-to-end test with:

```bash
RUN_BIGTABLE_EMULATOR_TESTS=1 \
BIGTABLE_EMULATOR_HOST=127.0.0.1:8086 \
cargo test --locked --features emulator-tests --test emulator
```

See the
[Google Bigtable emulator guide](https://cloud.google.com/bigtable/docs/emulator)
for emulator behavior and limits.

## Development

Run the same checks used in CI:

```bash
cargo fmt --all --check
cargo check --locked --workspace --all-targets --all-features
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-targets --all-features
cargo test --locked --workspace --doc --all-features
cargo check --locked --workspace --all-targets --no-default-features
cargo test --locked --workspace --all-targets --no-default-features
cargo test --locked --workspace --all-targets --all-features --release
cargo test --locked -p bigtable-client --lib --all-features --release \
  -- --ignored --test-threads=1
RUSTDOCFLAGS="-D warnings" cargo doc --locked --workspace --all-features --no-deps
cargo package --locked -p bigtable-client-derive
cargo package --locked -p bigtable-client \
  --config 'patch.crates-io.bigtable-client-derive.path="crates/bigtable-client-derive"'
```

The tests cover chunk assembly, resets and malformed streams, forward and
reverse resume ranges, deadlines, partial write retries, idempotency, bounded
concurrency, typed mapping, compiler diagnostics, and observability. The serial
tests exercise tracing and large read/write workloads. The separate emulator
test above exercises the raw and high-level APIs, mapping, deletes, and
concurrent reads from cloned clients.

Before merging, run the unit, public API, documentation, MSRV, release,
package, and emulator checks. Publishing remains a separate release action.

## License

Licensed under the
[MIT License](https://github.com/theutopialabs/bigtable-client-rs/blob/main/LICENSE).
