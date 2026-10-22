# Phase 4 tuning and candidate selection

The experiment uses the synthetic tuning split, not held-out final outcomes.
Vectors are controlled geometry, generation is a deterministic fake, and all
per-operation prices and external latencies are simulated. Measured local
execution does not establish production model quality or remote-cache savings.

## Reproduction

Build the declared data orders with `build-data -seed 101`, `202`, and `303`.
Then run:

```powershell
go run ./cmd/semcache tune -output results/phase4/tuning
go test ./internal/semanticcache -run '^$' -bench BenchmarkLSHTuning -benchmem -benchtime 100ms -count 3
```

Run benchmarks and replay separately to avoid CPU contention. `tune` requires a
new output directory and loads only `tuning-<seed>.json`. It rejects development
or final files, missing seeds, incorrect sizes, invalid grids and existing output
directories before running outcomes.

## Phase 4.1 method

The recorded grid in `configs/tuning_grid.json` crosses table counts 1, 2, 4 with
4 and 8 bits per table. Workload seeds are 101, 202, 303 and projection seeds are
1001, 2002, 3003. Every family is evaluated across their Cartesian product,
isolation/end-to-end replay, warm/cold start and capacities 100/1000. The same
conditions include always-search, always-skip and an exact-vector-repeat control.

Single-bucket prediction computes `L*K*dimension` hyperplane coefficients and
examines at most `L` scoped buckets, in increasing table order. Any occupied
bucket permits ordinary search; empty buckets permit bypass. Numerical
ambiguity, hashing failures and unready summaries search. No answer is returned
without cache retrieval and validation.

The exact-repeat control stores scoped SHA-256 fingerprints of normalized
float64 vectors and rebuilds membership from serialized snapshots. It stores
no answers and makes no cache calls while predicting. It is an exact-vector
membership reference, not a literal-text index or a semantic membership test.
Different text may have identical vectors, while repeated normalization can
change low-order float bits. Nearby semantic vectors need not match. No Bloom
filter is used.

The frozen requirements are recall at least 99% in every condition, total retained
summary memory at most 8 MiB, end-to-end p95 at most 1200 ms, paired p95 no higher
than always-search and strictly lower paired total serving cost. Isolation
serving counts describe the authoritative baseline; only end-to-end runs can
establish cost or latency benefit. Projection seeds are assessed together and
are never individually selected by favorable outcomes. Undefined recall fails
qualification. No statistical confidence or hardware-independent latency
guarantee is inferred from these deterministic fixtures.

`tradeoffs.csv` compares recall with lookup reduction, full structural summary
memory, retained heap, predictor time and cost for every seed/condition.
`sweep.json` retains all metrics; `report.md` and `assessments.json` explain
constraint failures. Paired evidence directories preserve input streams,
initial states, final states, oracle labels, response/mutation counts, actual
projection coefficients, and source/artifact checksums. Intermediate post-state
snapshot copies are omitted from the sweep JSONL; sequence numbers and the saved
deterministic input stream reproduce them. Ordinary `replay` keeps full states.

## Measurement boundaries

Summary memory includes projections, occupancy containers, entry bookkeeping,
vectors, keys and strings. The maximum incremental structural estimate and
measured retained heap must both fit the budget. A separate reference-cache
estimate and summary/reference ratio are reported with the same container
allowance; neither estimate is an exact heap measurement.

For each nonconstant predictor and condition, a separate, independently owned
summary is rebuilt at the state with the largest incremental structural estimate.
GC `HeapAlloc` before/after differences measure its retained heap, including
decoded strings and projection copies. Sampled before/after heap profiles permit
allocation attribution. These measurements describe a fresh rebuild at that
state; the incremental estimate also accounts for container high-water marks.
Heap profiling samples every 4096 allocated bytes, set near process startup and
recorded in the sweep manifest. Replay timings include this instrumentation.
Profiling, forced GC, file output and diagnostic oracle work take place outside
serving measurements.

Replay build time measures `Reset` and occupancy construction and is amortized
over the request count. Immutable hyperplane generation/persistence and adapter
construction are offline setup outside this serving-cost boundary. The frozen
cost result remains a simulated, instrumented local replay comparison.

Windows process working set is measured separately through
`GetProcessMemoryInfo`; Linux uses resident pages from `/proc/self/statm`.
Process values include the reference cache, evaluation snapshots, harness and Go
runtime, and must not be presented as summary size. Peak memory during rebuilding
is unmeasured here and remains part of later lifecycle/reconciliation work.

Benchmarks separately measure hashing, prediction and `Observe` reconciliation
on populated 100- and 1000-entry summaries, using projection seed 1001 and a
separate workload-vector seed 707. They report `ns/op`, bytes/op and allocs/op;
`Observe` benchmarks exclude snapshot extraction, which the serving path charges
separately. Allocations/op are not retained summary memory. CPU profiles are
saved separately from replay stage wall times.

The isolated Windows/amd64 benchmark capture used Go 1.26.3 on an AMD Ryzen 7
7445HS, three samples per case with a 100 ms benchmark duration. Median prediction
times across the populated 100/1000-entry cases were:

| Tables / bits | Median ns/op range | Bytes/op | Allocs/op |
| --- | ---: | ---: | ---: |
| 1 / 4 | 983.6–1051 | 640 | 2 |
| 1 / 8 | 1400–1649 | 640 | 2 |
| 2 / 4 | 1500–1562 | 768 | 2 |
| 2 / 8 | 2306–2359 | 768 | 2 |
| 4 / 4 | 2730–2791 | 1024 | 2 |
| 4 / 8 | 3971–4246 | 1024 | 2 |

Median `Observe` reconciliation took 219426–236258 ns for 100 entries and
2180622–2414783 ns for 1000 entries. Snapshot reconciliation scans the whole
cache; this is a visible maintenance cost even when the prediction itself is
small. These are local microbenchmarks, not production latency measurements.
Raw samples, their min/median/max summary and CPU profile are saved in
`results/phase4/benchmarks-v2.txt`, `benchmark-summary.csv`, and
`predictor-cpu-v2.pprof`. The earlier benchmark/partial-sweep capture overlapped
briefly and is excluded from selection and this summary.

## Phase status

Phase 4.1 is complete: 504 conditions, 504,000 requests and no serving/write
failures. Independent verification passed all 6342 artifact checksums and every
source hash; the sweep has one implementation digest. Tests, vet and race checks
passed. The 4-table/4-bit family achieves 100% recall in every declared condition,
so nearby-bucket probing is unnecessary.

No LSH family passes every frozen constraint. The 2-table/4-bit family passes
71/72 conditions (minimum recall 99.39024%) but fails paired cost once. The
4-table/4-bit family passes 65/72 conditions, failing paired p95 seven times.
The other families fail recall and/or cost. All measured summary-memory and
absolute-p95 checks pass. LSH retained heap is 116944–241816 bytes; maximum
structural estimates range from 177666 to 412361 bytes across families.
These observations do not establish production savings.

## Phase 4.2 frozen decision

`configs/final_candidate.json` freezes **always-search**, status
`no-qualifying-lsh`, after verifying every tuning artifact checksum. It contains
the exact experiment settings, embedding identity, assessments, evidence hashes
and freeze timestamp. Selection does not change budgets or choose a favorable
projection seed. Final evaluation remains unopened.

| Family | Minimum recall | Lookup reduction range | Maximum structural bytes | Passing conditions |
| --- | ---: | ---: | ---: | ---: |
| 1 table / 4 bits | 96.10% | 4.44–8.78% | 177666 | 25 / 72 |
| 1 table / 8 bits | 78.05% | 8.89–28.22% | 218068 | 0 / 72 |
| 2 tables / 4 bits | 99.39% | 3.22–5.33% | 226292 | 71 / 72 |
| 2 tables / 8 bits | 85.63% | 7.44–21.22% | 278884 | 3 / 72 |
| 4 tables / 4 bits | 100% | 2.33–3.22% | 322353 | 65 / 72 |
| 4 tables / 8 bits | 91.88% | 6.22–15.44% | 412361 | 31 / 72 |

Ranges span every declared mode/start/capacity/seed condition, rather than hiding
poor cases behind pooled means. Per-seed metrics and paired differences remain
in the CSV/JSON evidence. `results/phase4/seed-summary.csv` also reports
min/median/max and sample counts separately per variant/mode/start/capacity,
using all nine workload/projection seed pairs for LSH and three orders for each
control; its reproducible inspection script is `summarize-seeds.ps1` alongside
the CSV. In the failing 2-table/4-bit condition (workload 303,
projection 2002, end-to-end warm start, capacity 100), recall is 818/819 but
generation increases by two calls and simulated cost increases by
$0.000143412755. Across naturally diverged caches, false negatives alone do not
equal the extra generation count. The 4-table/4-bit latency failures use the
strict predeclared paired comparison; short local timings and their quantization
do not support a statistical production-latency claim. No threshold was relaxed.

Reproduce the selection from saved evidence with:

```powershell
go run ./cmd/semcache freeze-candidate -tuning results/phase4/tuning-v2 -output results/reproduced-candidate.json
go run ./cmd/semcache replay -candidate configs/final_candidate.json -data data/generated/development-101.json -start warm -output results/fallback-check
```

The freeze command verifies checksums and matrix completeness, recomputes the
constraint assessment, and prefers the lowest total simulated Mode B serving
cost among qualifying families, with lexical order for exact ties. It refuses to
overwrite a frozen output. The saved fallback replays only always-search; a
qualified LSH file would replay the baseline plus every frozen projection seed,
checking coefficient identities. Candidate replays preserve the exact JSON as a
checksummed artifact. The development-only smoke replay passed. Later robustness
and break-even work remains appropriate, without a current benefit claim.

## Debugging audit

The Phase 4 audit reproduced two validation defects. Matrix assessment accepted
an empty grid and could accept LSH/control rows with incorrect table/bit metadata.
It now validates the grid and checks each variant against its declared shape.

Candidate freezing previously checked the tuning split only for the first row
and trusted the sweep's copies of metrics and paired baselines. Regression tests
showed that a later final-split workload, missing evidence, changed paired cost,
changed metrics or an altered request trace could still freeze a candidate even
after the files had been rechecksummed. Freezing now verifies every row against
its workload, configuration, manifest, projection identity and paired metrics,
and recomputes metrics from both routers' saved request traces. The recorded
top-level grid and configuration must also match the sweep. Source identities
must agree across the capture; historical evidence need not match the current
auditing code revision. These checks run offline during freezing.

Audit commands and before/after regression evidence are saved under
`results/phase4-debugging/`; `docs/progress.md` records verification results.
The original frozen candidate and original tuning measurements are preserved.
