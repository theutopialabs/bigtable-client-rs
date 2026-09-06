# Test cleanup and local client comparison

Measured September 5–6, 2026. All work and commits remain local.

Our client was generally 3–11% behind `bigtable_rs` in the short small-operation throughput cases, with higher client CPU and memory use. Both direct gRPC clients sustained approximately 20,000 offered point reads/s and completed the bounded stress workloads without request errors. The `rust-bigtable` REST path was slower and suffered connection failures under sustained load. Its measurements include a required REST-to-gRPC bridge, so they compare complete local paths rather than isolated libraries.

## Test changes and verification

Removed or consolidated 27 redundant test cases: 18 internal cases and nine public API cases. The removed assertions checked assigned fields, derived clone/debug behavior, nonempty display strings, or scenarios already covered by stronger behavioral tests. Representative numeric decoding now checks unsigned, signed, fractional, and invalid input behavior instead of repeating the same value across primitive widths. The credential-redaction test now uses an authenticated interceptor containing a real test secret, making its assertion meaningful.

Retained coverage exercises query and mutation validation, binary data, row assembly, mapping and derive diagnostics, retry classification and original indices, partial success, deadlines, backpressure, cancellation, transport limits, and live emulator behavior. Production code is unchanged: the production portions of all 11 touched source files were compared against `2e8f220`. See [the change audit](benchmarks/2026-09-05/change-audit.json).

Rust 1.88.0 verification passed with warnings denied: formatting, workspace checks and Clippy, all-feature and no-default-feature tests, release tests, doctests, rustdoc, package verification for both crates, four serial ignored stress/instrumentation tests, and the enabled live emulator test. The full suite ran after the main cleanup; the final five public API deletions were followed by the affected test target in both feature modes, formatting, and targeted Clippy. Final counts are 131 internal tests with all features (130 without defaults), seven public API tests in either mode, plus the derive UI, transport, emulator, doctest, and explicit stress checks. No new test suite was added for the removed assertions. [Required checks](benchmarks/2026-09-05/required-checks.json) · [Final focused checks](benchmarks/2026-09-05/final-checks.json).

## Reproduction and controls

The standalone harness is at [bt-client-comparison-20260905](/Users/vishnu/Documents/testing/bt-client-comparison-20260905/README.md), outside the library workspace. It includes independent locked adapter crates, the bridge, table control, build/run scripts, raw traces, resource samples, and analysis. The complete harness and evidence are committed locally at `c67598b`. The local client was measured at `8beb48c91c78fb0e962d59022d09644db1f9852b`; later changes only remove tests or add these artifacts. Runtime harness source was frozen at `a09d6f9`; the supervisor was subsequently corrected to retain timeout output and resume unfinished trials with identical client binary hashes.

| Client | Pinned source | Path |
| --- | --- | --- |
| Ours | `8beb48c91c78fb0e962d59022d09644db1f9852b` | Direct gRPC; default OpenTelemetry feature, no exporter |
| `bigtable_rs` 0.4.1 | [9557654](https://github.com/liufuyang/bigtable_rs/tree/9557654455e47638594bfae665c7379e1a3447e7) | Direct gRPC; ring auth/TLS features, inactive on plaintext emulator |
| `rust-bigtable` 0.6.0 | [f80989f](https://github.com/durch/rust-bigtable/tree/f80989fdd1c24c4aeace967c553a44af9eb8a184) | Unmodified blocking libcurl HTTP/JSON client through a streaming bridge |

The host was an Apple M5 Pro with 18 cores and 24 GiB RAM, running macOS 26.6.2. Colima exposed six CPUs and about 12 GB RAM; the dedicated emulator container had a 4 GiB limit. All adapters used Rust 1.88 release builds, four Tokio workers, and one gRPC channel for each native client or bridge. The emulator image was `google-cloud-cli:574.0.0-emulators`, local image ID `sha256:e00503d9c7ad772497208104e32988cc562eb7d7d31d1cb5b90a96b6b304f9f6`.

Ten workloads × three clients × three rotated fresh-process repetitions produced 90 retained trial results. Every trial had a 0.2-second excluded warmup. The usual fixture was 4,096 cyclic keys, one 256-byte cell per row, and fixed timestamps; writes overwrote the bounded fixture. Every timed read validated complete cell contents and every mutation checked statuses. Full-fixture hashes were checked before and after timing, with independent recovery reads when the REST transport prevented postflight verification.

Tables below show median throughput and median trial p99. Latency includes response materialization and validation; open-loop latency starts at scheduled arrival. Error operations contribute latency samples; dropped arrivals do not. [All individual trials](benchmarks/2026-09-05/summary.csv) and [full aggregates](benchmarks/2026-09-05/aggregate.csv) retain ranges, p50/p95/p99, CPU, memory, errors, and drops.

## Short performance trials

One-second closed-loop phases. Entries are successful rows/s, followed by p99 operation latency in milliseconds. A write operation contains 100 rows.

| Workload | Ours | `bigtable_rs` | `rust-bigtable` + bridge |
| --- | ---: | ---: | ---: |
| Point read, concurrency 1 | 3,697 / 0.423 | 3,857 / 0.435 | 2,329 / 0.643 |
| Point read, concurrency 8 | 17,866 / 0.795 | 18,401 / 0.767 | 9,572 / 1.247 |
| Batch write, concurrency 1 | 198,924 / 0.923 | 222,926 / 0.895 | 60,330 / 2.463 |
| Batch write, concurrency 8 | 751,379 / 1.903 | 800,832 / 1.815 | 313,025 / 3.711 |

Our median throughput was 4.1%, 2.9%, 10.8%, and 6.2% lower than `bigtable_rs`, respectively. Some trial ranges overlap, especially the single-worker write case; these short local trials do not establish a universal ranking. All short performance trials had zero request errors or drops.

## Offered-load trials

Five-second point-read phases with concurrency capped at 64. Each cell shows successful operations/s, p99 milliseconds, and median error/drop percentages using all offered arrivals as the denominator.

| Offered operations/s | Ours | `bigtable_rs` | `rust-bigtable` + bridge |
| --- | ---: | ---: | ---: |
| 1,000 | 1,000; 2.319; 0% / 0% | 1,000; 2.335; 0% / 0% | 1,000; 2.383; 0% / 0% |
| 5,000 | 4,998; 2.863; 0% / 0% | 4,999; 2.623; 0% / 0% | 3,023; 30.847; 35.41% / 4.02% |
| 20,000 | 19,991; 3.135; 0% / 0%* | 19,992; 3.087; 0% / 0.016%* | 2,534; 33.535; 19.16% / 68.14% |

*Small median percentages hide variation: across the three 20,000/s trials, ours dropped 38 of 300,000 scheduled arrivals (0.0127%), and `bigtable_rs` dropped 46 (0.0153%). Neither had request errors. The scheduler's maximum arrival lag was about 2.3 ms for these native cases, so open-loop p99 should not be read as RPC service time alone. The REST error percentage decreases at 20,000/s because many more arrivals are dropped before execution, not because reliability improves.

## Stress trials

Each cell shows successful operations/s and p99 milliseconds. The 1,000-row read uses explicit row keys, rather than a contiguous range scan. Mixed operations each read or write 100 rows; every worker alternates between the two.

| Workload | Ours | `bigtable_rs` | `rust-bigtable` + bridge |
| --- | ---: | ---: | ---: |
| 1,000-row reads, 10 s, concurrency 8 | 507 / 20.095 | 530 / 18.943 | 343 / 30.207 |
| 1 MiB cell reads, 10 s, concurrency 8 | 469 / 22.655 | 454 / 24.063 | 242 / 45.311 |
| Mixed 100-row operations, 30 s, concurrency 64 | 7,233 / 15.423 | 7,294 / 14.527 | 531 / 35.839 |

The first two stress cases had zero request errors for every client. Native mixed-operation trial ranges overlap substantially: ours 6,871–7,400 ops/s versus `bigtable_rs` 6,606–7,426. Both had zero request errors. Our large-cell median was 3.4% higher, also with overlapping ranges. The REST mixed case had a median 87.14% request-error rate and one retained process timeout.

### CPU and memory

Median sampled peak client RSS, in MiB:

| Workload | Ours | `bigtable_rs` | REST client / client plus bridge |
| --- | ---: | ---: | ---: |
| Point reads, concurrency 8 | 10.4 | 9.3 | 13.9 / 27.8 |
| Batch writes, concurrency 8 | 16.0 | 10.6 | 17.9 / 36.3 |
| 1 MiB cell reads | 55.1 | 49.4 | 107.5 / 204.4 |
| Mixed stress | 37.1 | 22.5 | 42.2 / 87.7 |

For concurrency-eight batch writes, client CPU per successful 100-row operation was 177 µs for ours, 133 µs for `bigtable_rs`, and 566 µs for the REST client plus 1,009 µs in the bridge. Mixed-stress client CPU was 366 µs versus 280 µs for the native clients. These measurements locate useful profiling targets, but do not identify which internal checks or allocations cause the difference. RSS is sampled at approximately 10 ms; bridge and emulator memory can retain allocations between workloads. Resuming restarted the bridge while preserving the emulator, so bridge allocation history changed at the mixed-stress boundary.

## Failures, result integrity, and limits

The audit reconciled 3,141,349 offered operations across 90 retained trials: 2,525,413 successes, 404,834 request errors, and 211,102 dropped arrivals. All request errors were in the REST path. No timed-read data mismatches were recorded. Every preflight and postflight or independent recovery hash matched its fixture; eight REST trials required transport recovery for the final hash. See [audit.json](benchmarks/2026-09-05/audit.json).

A separate diagnostic observed 16,080 task endpoint sockets in `TIME_WAIT` against this Mac's 16,384-port ephemeral range, followed by connection failures while the bridge stayed alive. The unchanged REST client creates a new libcurl handle per request. The harness drains task sockets between trials to prevent one trial poisoning the next; exhaustion within sustained trials remains a measured failure. This host-specific failure cannot be generalized to production Bigtable.

One earlier REST mixed-stress attempt hit the 180-second process cap after its measurement-end marker. The original supervisor lost its timing/counter output; its marker trace is preserved under `results/matrix/attempts/` in the harness. The supervisor was corrected and that slot rerun, while retaining the other 83 finished results and unchanged benchmark binaries. Thus there were 90 retained trial results plus that earlier failed attempt. The replacement also timed out after emitting final JSON, so its measurements and timeout are both included. Two process timeouts were observed in total. The unchanged client's blocking libcurl work can outlive an async timeout and delay Tokio runtime shutdown; this is consistent with the retained final JSON followed by failure to exit, rather than a missing measurement interval. Successful process exits and supervisor timeouts are checked separately by the analyzer.

These results cover a local emulator without production authentication, TLS, WAN latency, quotas, replication, or service scaling. They cover bounded synthetic overwrites and up to 30-second measured stress intervals, not long-duration leak testing or production capacity. The bridge adds protocol conversion and memory costs to the REST path. No optimization or production-superiority claim is warranted from this comparison alone.

The dedicated emulator was removed and its absence verified by exact container ID and name. All harness sources, lockfiles, raw measurements, failed-attempt traces, validation logs, and cleanup evidence remain local in the standalone harness repository.
