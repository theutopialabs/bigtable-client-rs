# Release cleanup audit

Baseline: `184f774` on local branch `vishnu/release-cleanup`.

This audit covers both crates, their public API, request and stream lifecycles,
authentication, configuration, mapping, observability, tests, documentation,
dependencies, and release workflows. Changes stay local. The development API
may change when that produces a simpler, correct contract.

## Priorities

| Priority | Finding | Plan and proof | Status |
| --- | --- | --- | --- |
| P1 | Production configuration accepts HTTP although the client attaches credentials. | Require HTTPS for production endpoints; select TLS from the effective URI scheme. Test configuration loading, builders, and local handshake behavior. | In progress |
| P1 | A full read buffer bypasses the operation deadline and retains its RPC. | Bound row delivery by the operation deadline and release the RPC before terminal error delivery. Use paused time and an observed stream drop. | In progress |
| P1 | Deriving a mapper for `r#type` selects `r#type` instead of `type`; a qualified custom `Option` is misclassified as sparse. | Normalize raw identifiers and recognize only supported standard Option paths. Exercise runtime mapping and compiler fixtures. | In progress |
| P2 | Deadline and malformed-response mutation failures lose the entry's known replay safety. | Keep idempotency on the failed entry, independently of its failure cause. Exercise safe and unsafe writes across failure paths. | In progress |
| P2 | Accepted extreme retry durations can panic during floating-point conversion. | Saturate conversion at the configured maximum and test duration/multiplier overflow. | In progress |
| P2 | Retry diagnostics count scheduled retries that can be cancelled before an RPC starts. | Count retries when an attempt starts. Test cancellation during scheduled backoff and actual subsequent attempts. | In progress |
| P2 | Row mapping performs binary search without a sorted-column invariant, followed by linear fallback. | Use the lookup that matches the documented server-order row model; test unsorted columns. | In progress |
| P2 | Token refresh owns a mutex that no concurrent caller uses. | Store the task handle directly and retain cancellation on manager drop. Check short-lived provider behavior before changing refresh scheduling. | Under review |
| P2 | README repeats milestone history and capability lists; development commands omit some locked CI checks. | Keep user workflows, limits, release requirements, and one accurate validation recipe. | Planned |

Plans are checked with the thirdeye skill before implementation. Completed
changes receive autoreview against the actual diff, callers, failure paths,
and relevant regression tests before being committed.

## Investigation notes

- Scan-marker sequencing is not yet a confirmed defect. Go and Java clients
  handle marker/chunk ordering differently; check protocol conformance before
  changing accepted stream behavior.
- Wide-row assembly performs repeated column searches. Measure a representative
  workload and preserve server order before choosing an optimization.
- Existing bulk retry safeguards, bounded request concurrency, typed errors,
  authentication sharing, and channel-pool validation protect real behavior;
  their presence alone is not a reason to remove them.

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
