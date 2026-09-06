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
| P2 | [Token refresh](../crates/bigtable-client/src/auth.rs) can rapidly poll near-expiry tokens; its task handle has an unnecessary mutex. | Refresh short-lived tokens partway through their TTL; retain the five-second fallback for failed/non-advancing refreshes and direct task ownership. Tests reproduced 2,580 calls during 20 ms of clock advancement before the fix. | Fixed: `d10472a`, `9fba697` |
| P2 | [Wide-row assembly](../crates/bigtable-client/src/merge.rs) repeatedly searches every existing column. | Lazily index families with 16 or more columns; keep the ordered public row model and narrow-row lookup. Tests cover interleaving, versions, commit, and reset. | Fixed: `b9585a1` |
| P2 | [README](../README.md) repeats milestone history and omits some locked development checks. | Keep workflows, limits, release requirements, and one validation recipe; correct buffering, TLS, and failure-safety descriptions. | Fixed |

Plans are checked with the thirdeye skill before implementation. Completed
changes receive autoreview against the actual diff, callers, failure paths,
and relevant regression tests before being committed.

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

## Validation

- Initial all-feature baseline: 135 unit tests, 16 public API tests, and the
  derive compiler suite passed; four serial instrumentation/stress tests were
  intentionally excluded from that run.
- The live local emulator baseline passed separately with its required opt-in.
- Rust 1.88.0 all-target/all-feature compilation passed. The shell overrides
  the repository toolchain with Rust 1.97.1, so subsequent release checks use
  `cargo +1.88.0` explicitly.
- Final feature, compiler, documentation, release, packaging, and emulator
  verification will be recorded after the changes are integrated.
- `cargo audit` is not installed; no vulnerability-audit result is claimed.

Local emulator checks establish emulator behavior, not production Bigtable
behavior. This audit does not publish crates, push commits, create tags, or
change hosted CI or release state.
