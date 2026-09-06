# Release cleanup audit

Baseline: `184f774` on local branch `vishnu/release-cleanup`.

This audit covers both crates, their public API, request and stream lifecycles,
authentication, configuration, mapping, observability, tests, documentation,
dependencies, and release workflows. Changes stay local. The development API
may change when that produces a simpler, correct contract.

## Priorities

| Priority | Finding | Plan and proof | Status |
| --- | --- | --- | --- |
| P1 | [Production configuration](../crates/bigtable-client/src/config.rs) accepts HTTP although the client attaches credentials. | Require HTTPS for production endpoints; select TLS from the effective URI scheme. Test loading, builders, and a local TLS handshake. | Fixed: `63e3bf3` |
| P1 | [Bearer metadata](../crates/bigtable-client/src/auth.rs) is not marked sensitive, allowing debug output to expose it. | Mark metadata sensitive and verify redaction using a synthetic token. | Fixed: `d10472a` |
| P1 | The [generated client configuration](../crates/bigtable-client/src/client.rs) inherits a 4 MiB decoding limit. | Set the 256 MiB response bound. A real local gRPC test reproduced rejection of a 5 MiB response, then passed. | Fixed: `717a881` |
| P1 | A full [read buffer](../crates/bigtable-client/src/read.rs) bypasses operation and attempt deadlines. | Reserve delivery capacity before committing a row, honor both deadlines, and release the RPC before retry/error delivery. Paused-time tests verify cancellation and resume without duplicates. | Fixed: `168d4bf`, `405e430` |
| P1 | [Derive](../crates/bigtable-client-derive/src/lib.rs) selects `r#type` instead of `type`; a qualified custom `Option` is misclassified as sparse. | Normalize raw identifiers and recognize only supported standard Option paths. Runtime mapping and compiler fixtures pass. | Fixed: `c98b199` |
| P2 | Deadline and malformed-response [mutation failures](../crates/bigtable-client/src/error.rs) lose known replay safety. | Move idempotency onto the failed entry, independently of its cause. Safe/unsafe write and failure-path regressions pass. | Fixed: `4859ed0` |
| P2 | Extreme accepted [retry durations](../crates/bigtable-client/src/retry.rs) panic during floating-point conversion. | Saturate at the configured maximum; exercise duration/multiplier overflow. | Fixed: `8923271` |
| P2 | [Retry diagnostics](../crates/bigtable-client/src/telemetry.rs) count retries cancelled before the RPC starts. | Count retries when an attempt starts. Cancellation and actual retry-event tests pass. | Fixed: `5b59511` |
| P2 | [Token refresh](../crates/bigtable-client/src/auth.rs) can rapidly poll near-expiry tokens or sleep past a cached token's expiry; its task handle has an unnecessary mutex. | Refresh short-lived tokens partway through their TTL; cap successful cached-token retries at remaining validity, with a five-second fallback after expiry/errors. Tests reproduced both rapid polling and the validity gap before the fixes. | Fixed: `d10472a`, `9fba697`, `6724f3e` |
| P2 | [Wide-row assembly](../crates/bigtable-client/src/merge.rs) repeatedly searches every existing column. | Lazily index families with 16 or more columns; keep the ordered public row model and narrow-row lookup. Tests cover interleaving, versions, commit, and reset. | Fixed: `b9585a1` |
| P2 | [README](../README.md) repeats milestone history and omits some locked development checks. | Keep workflows, limits, release requirements, and one validation recipe; correct buffering, TLS, and failure-safety descriptions. | Fixed |

Plans were checked with the thirdeye skill before implementation. Changes
received autoreview against the actual diff, callers, failure paths, and
regression tests before being committed. Independent review caught the attempt
deadline and cached-token validity gaps; both were corrected and reverified.

## Investigation notes

- Scan-marker changes were withdrawn. Google's [Java state machine](https://raw.githubusercontent.com/googleapis/java-bigtable/main/google-cloud-bigtable/src/main/java/com/google/cloud/bigtable/data/v2/stub/readrows/StateMachine.java)
  matches the current strict behavior; [Go](https://raw.githubusercontent.com/googleapis/google-cloud-go/main/bigtable/bigtable.go)
  differs, and shared conformance cases do not settle mixed marker/chunk
  sequencing. The evidence does not justify changing protocol acceptance.
- Row mapping's binary search plus fallback was retained after review: it
  provides fast lookup for sorted columns and correct fallback for public
  unsorted rows. Replacing it with linear search would regress wide-row lookup
  without an established correctness benefit.
- The 256 MiB response bound follows Google's [Java client settings](https://raw.githubusercontent.com/googleapis/java-bigtable/main/google-cloud-bigtable/src/main/java/com/google/cloud/bigtable/data/v2/stub/EnhancedBigtableStubSettings.java).
- Existing bulk retry safeguards, bounded request concurrency, typed errors,
  authentication sharing, and channel-pool validation protect real behavior;
  their presence alone is not a reason to remove them.

## Wide-row measurement

Three rotated fresh-process trials used Rust 1.88, three warmups per case,
optimized copies of the actual merger source, and the same existing debug
dependencies. Median time per assembled row:

| Columns × versions | Before | After |
| --- | --- | --- |
| 8 × 1 | 2.37 µs | 2.39 µs |
| 8 × 8 | 10.78 µs | 11.10 µs |
| 1,000 × 1 | 4.72 ms | 0.57 ms |
| 4,000 × 1 | 74.06 ms | 2.28 ms |
| 16,000 × 1 | 1,172.76 ms | 9.24 ms |

An unconditional index slowed the narrow case, so the final implementation
builds indices lazily. Additional memory scales with indexed columns. Output
checks and order/reset regressions passed. These are local CPU measurements,
not network throughput or production latency claims. Source snapshots, the
reproduction script, and raw CSV results are retained for this local session
in `/tmp/bigtable-merger-audit/` (`python3 reproduce.py`).

## Final validation

All checks below passed on source commit `12feb9f`, using Rust 1.88.0 explicitly
because the shell overrides the repository toolchain with Rust 1.97.1. The
subsequent commit updates only this audit record. The full matrix was rerun
after the final authentication correction; the working tree was clean during
packaging.

| Check | Result |
| --- | --- |
| Formatting, all-target compilation, Clippy | Passed with warnings denied; all-feature and no-default-feature compilation covered. |
| All-feature debug tests | 149 unit tests, 16 public API tests, derive compiler fixtures, and the real gRPC transport regression passed. |
| No-default-feature tests | 148 unit tests, 15 public API tests, derive compiler fixtures, and transport regression passed. |
| Release tests | Same all-feature test suite passed with optimization. |
| Serial instrumentation/stress tests | All four normally ignored tests passed separately: large streams, split cells, partial bulk retries, and span hierarchy. |
| Rustdoc | Three doctests passed with each feature configuration; documentation built with warnings denied. |
| README examples | All 11 examples passed Rustdoc verification against the built client. |
| Packages | Both crates packaged and their extracted packages compiled with `--locked`; client validation used the local derive patch from CI. |
| Live local emulator | Raw/high-level reads, writes, deletes, typed mapping, observability, and concurrent reads passed with the opt-in enabled. |
| Cleanup and diff | The task's emulator container was removed and its absence verified. `git diff --check` passed. |

The commands match the README development recipe, with `cargo +1.88.0`,
`RUSTFLAGS=-Dwarnings`, and `RUSTDOCFLAGS=-Dwarnings`. Logs, the sequential
validation script, and its JSON results are retained for this local session in
`/tmp/bigtable-release-cleanup-validation/`.

`cargo audit` is not installed; no vulnerability-database scan is claimed.

Local emulator checks establish emulator behavior, not production Bigtable
behavior. This audit does not publish crates, push commits, create tags, or
change hosted CI or release state.
