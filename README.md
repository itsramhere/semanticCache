# Semantic Cache

A Go prototype for reusing answers to semantically similar FAQ requests. Requests are embedded as vectors, matched against a scoped cache, and served from the cache only after the stored answer passes validation. A miss generates a new answer and caches it when eligible.

The research goal is to determine whether a local predictor can skip unproductive semantic-cache searches cheaply enough to reduce total serving cost. **Phases 1 through 5 are complete, including debugging and tests:** the baseline, validated synthetic workloads, read-only oracle, paired replay modes, counted LSH predictor, tuning measurements, a frozen candidate decision, versioned lifecycle events and stale-summary recovery. No LSH family passed every frozen tuning constraint; the selected serving configuration uses always-search. Phase 5 validates lifecycle consistency and recovery within the prototype scope. The experiments have not demonstrated a performance benefit.

## How it works

```text
Request -> embed -> baseline: search, or LSH: predict -> search if allowed
                                                        | match: validate -> return cached answer if valid
                                                        | miss/rejected/skipped: generate -> cache if eligible
```

Noncacheable requests still compute an embedding and generate an answer, but bypass cache reuse and insertion. The fake LLM produces deterministic simulated answers; it does not call a real language model.

The baseline includes:

- Exact cosine search using normalized `float64` vectors.
- Isolation by namespace, tenant, locale, product version, knowledge version, and generation model version.
- Insert, replace, delete, revoke, expire, and FIFO eviction operations.
- An injected clock for deterministic replay and expiration tests.
- Revalidation of retrieved answers, including scope, lifetime, validity, and similarity.
- Explicit errors for invalid embeddings, failed searches, and failed generation. A cache-write failure is reported separately from a successful generated answer.
- Per-request stage timings, response sources, operation counts, reusable embeddings, and run manifests.

Cache access and replay are serialized. The current cache is in memory and does not persist across runs.

## Requirements

- Go **1.26.3 or later**.
- A compatible C compiler and cgo for race-detector tests.
- An optional Hugging Face Text Embeddings Inference (TEI) server for live embeddings.

The application uses the Go standard library. The default demo requires no API key, Python installation, Docker, or running model server.

## Quick start

Run these commands from the repository root:

```powershell
go run ./cmd/semcache demo
```

The demo starts with an empty cache and serves four synthetic FAQ requests. The first generates an answer, the repeated request hits the cache, and the two new requests generate answers. Expected operation counts are four searches, three generation attempts, and three successful writes.

The default demo uses three recorded embeddings from real CPU inference with `sentence-transformers/all-MiniLM-L6-v2`, plus simulated fake-LLM answers. It writes the following files to `results/demo`:

| File | Contents |
| --- | --- |
| `requests.jsonl` | Request results, timings, errors, and operation counts |
| `embeddings.json` | Reusable vectors with model and preprocessing identity |
| `workload.json` | Ordered requests and logical timestamps |
| `config.json` | The configuration used for the run |
| `manifest.json` | Toolchain, platform, embedding identity, and source/artifact hashes |

Repeating the command overwrites that output directory. Use a different directory to keep separate runs:

```powershell
go run ./cmd/semcache demo -output results/my-demo
```

Run the demo from within the source checkout so the manifest can identify the code. The configuration file may be stored elsewhere.

## Configuration and commands

The default configuration is [configs/experiment.json](configs/experiment.json). The demo uses the first configured cache capacity.

| Setting | Default |
| --- | --- |
| Matching policy | `faq-cosine-v1` |
| Cosine threshold | `>= 0.90`, inclusive |
| Entry lifetime | 3,600 logical seconds from insertion |
| Cache capacities | 100 and 1,000 entries; demo uses 100 |
| Eviction | Global FIFO across scopes |
| Embedding model | `sentence-transformers/all-MiniLM-L6-v2` |
| Embedding dimension | 384 |

Matches must have identical scope and valid, reusable, unrevoked answers. Entries expire exactly at their expiration timestamp. Highest similarity wins; exact score ties use the lexicographically smallest entry ID. Replacing an entry resets its lifetime and moves it to the FIFO tail. Searches do not refresh either lifetime or FIFO position.

```powershell
go run ./cmd/semcache validate-config -config configs/experiment.json
go run ./cmd/semcache verify-policy
go run ./cmd/semcache version
go run ./cmd/semcache help
go build ./cmd/semcache
```

`validate-config` rejects unknown fields, trailing JSON, missing settings, and invalid values. `verify-policy` runs 14 hand-vector examples. Demo options are `-config`, `-output`, and `-tei`.

## Phase 2: evaluation harness

Build all three splits, validate them together, then replay development data:

```powershell
go run ./cmd/semcache build-data -seed 101
go run ./cmd/semcache validate-data data/generated/development-101.json data/generated/tuning-101.json data/generated/final-101.json
go run ./cmd/semcache replay -data data/generated/development-101.json -mode isolation -start warm -output results/isolation
go run ./cmd/semcache replay -data data/generated/development-101.json -mode end-to-end -start cold -output results/end-to-end
```

Repeat `build-data` with `-seed 202` and `-seed 303` for the other declared orders.
The [data documentation](data/README.md) records origin, split definitions,
initial states and a representative label review. Validation rejects duplicate
IDs, split leakage, missing metadata, invalid vectors and backward timestamps.
Whitespace-only identifiers and labels are treated as missing metadata.
Supply all split paths together to check leakage across files. Keep final
replays unopened until candidate selection is frozen.

These evaluation fixtures use **simulated controlled vectors**, with a distinct
64-dimensional model identity and [configuration](configs/phase2_fixture.json).
They test the harness rather than MiniLM quality. The Phase 1 demo continues to
use recorded real MiniLM vectors. Fake LLM generation is simulated in both.

Replay compares always-search with always-skip. Mode A (`isolation`) evaluates
decisions against identical authoritative baseline snapshots and updates;
serving counts in this mode describe the baseline path. Mode B (`end-to-end`)
uses independent caches that may diverge. Its oracle labels each request using
that run's own pre-decision state. Both modes reset state between runs and
validate retrieved answers with the baseline policy.

Replay flags are `-data` (required), `-config` (defaults to the fixture config),
`-mode`, `-start` (`warm` or `cold`), `-capacity` (default 100), `-output`,
`-oracle-every` (default 1; 0 disables diagnostics), and `-oracle-log` (default
true). Oracle sampling/logging affects diagnostic counts and available labels;
regression tests verify unchanged serving responses, decisions and mutations.

Each run saves paired JSONL traces, final states, exact workload/config inputs,
`metrics.json`, `metrics.csv`, and a manifest with source/artifact hashes, Git
revision/status, Go version, seed and command. Saving rejects duplicate predictor IDs,
mismatched paired request streams/options, and input files that differ from
the evaluated configuration or workload. Timings are measured local
routing with precomputed vectors. End-to-end latency estimates and serving
cost are explicitly simulated using the saved assumptions; no artificial
delays are slept. Oracle, snapshot copying and logging are outside serving
timings and counters. Build time is reported separately and amortized in
cost/estimated latency. Memory is zero for these constant controls, which have
no summary; process/rebuild memory is `N/A` until implemented/measured.

Metrics report TP/FP/FN/TN, lookup reduction, hit recall, missed-hit fraction,
unsuccessful routed searches, correctly avoided misses, operation counts,
failures, p50/p95, predictor/update/build time and paired cost/generation
differences. Zero denominators are `N/A`. Answer-reuse accuracy uses authored
answer classes and scope, separately from vector-match routing recall. The
near-match fixtures deliberately expose incorrect reuse despite a vector hit.
These checks establish evaluation behavior, without a predictor benefit claim.

## Phase 3: LSH predictor

Persist seeded projections, then include the predictor in a development replay:

```powershell
go run ./cmd/semcache build-data -seed 101
go run ./cmd/semcache build-projections -config configs/lsh_development.json -output results/phase3/projections.json
go run ./cmd/semcache replay -data data/generated/development-101.json -lsh results/phase3/projections.json -mode end-to-end -start warm -output results/phase3/end-to-end-warm
```

Use `-mode isolation` to evaluate against authoritative baseline snapshots, and
`-start cold` for an empty initial cache. Omit `-lsh` to replay only the controls.
The example configuration uses two tables, eight bits and projection seed 1001;
it is a development implementation check, not a tuned or frozen final candidate.
`build-projections` accepts `-config` and `-output`. Its strict JSON config contains
`version` (`cosine-hyperplanes-v1`), full embedding `identity`, `seed`, `tables`
(1–256), and `bits` (1–64), with a 16-million-coefficient resource limit.

Hashing normalizes vectors, sets bit b when its hyperplane dot product is >= 0,
and probes one bucket per table. Any occupied bucket permits a search; empty
buckets permit a bypass. Keys contain the full scope, projection identity, table
and bucket. The identity hashes configuration and actual coefficients. Saved
projection files are reloaded without regeneration and are included in replay
checksums alongside the seed, workload and source identity.

If a dot product falls within the float64 roundoff bound (eight machine epsilons
per dimension times the sum of absolute products), prediction searches with
`hash-boundary-fallback`. This protects exact repeats when normalization changes
the last bits near a hyperplane; stored keys still use the documented >= 0 rule.

Occupancy uses counts and entry-to-key bookkeeping so shared buckets survive
deletion of one contributor. The end-to-end serving path uses one serialized
cache/summary mutation path for inserts, replacements, deletes, revocation,
expiration and FIFO eviction. Prediction reads only the local summary. Unready
summaries and hashing failures search; maintenance failures immediately disable
skipping until `SynchronizedCache.Rebuild()` succeeds. Initial construction builds
the summary before enabling it. Programmatic callers transfer cache ownership to
`NewSynchronizedCache`, then set `Baseline.Cache` and `Baseline.Predictor` to that
wrapper and its summary; all mutations must go through the wrapper. Isolation
mode consumes the harness's authoritative snapshot stream instead.
Serving also checks the summary's full embedding identity and its association
with the active cache. A mismatch searches with an explicit fallback reason.

Synchronous maintenance, including its snapshot copying, is charged to update
time and serving cost. Memory is a conservative structural estimate including
projections, vectors, entry ledger, keys, strings and map/container allowances;
retained heap and separate process measurements are added by Phase 4. Phase 5
adds separate rebuild memory measurements with explicit sampling limits.

LSH can skip real semantic matches. A regression separates vectors `(0.1, 1)`
and `(-0.1, 1)` using the x=0 plane despite cosine similarity about 0.98. The
development warm replay also records missed hits in per-request JSONL and
confusion metrics. These controlled vectors and simulated costs do not establish
real embedding quality or a benefit. Phase 4 tuning is reported below; final
evaluation remains pending.

Phase 3 implementation and debugging checks are complete. The audit passed
tests, vet, race detection, build, and 24 development replay conditions across
workload seeds 101/202/303, both modes, warm/cold starts, and capacities 100/1000:
72 router runs and 7,200 requests, with zero serving or cache-write failures.
Regression tests cover unsynchronized and incompatible-summary fallback,
floating-point exact-repeat misses, and recovery after maintenance failure.

The current development configuration does **not** meet the provisional 99%
cache-hit recall target. Its end-to-end results across those workload seeds are:

| Initial cache | LSH hit recall | Searches: baseline / LSH | Generations: baseline / LSH |
| --- | --- | --- | --- |
| Warm | 97.14% | 90 / 70 | 30 / 32 |
| Cold | 96.67% | 90 / 61-62 | 40 / 42 |

Counts are per 100-request run, with 90 cache-eligible requests. Every end-to-end
LSH run increases simulated serving cost under the saved assumptions. Isolation
recall can differ because it follows baseline cache updates; cold seed 303
records 90% recall. Passing implementation checks establishes Phase 3 behavior;
it does not establish a useful recall/cost tradeoff. Phase 4 measures that tradeoff
using tuning data before any held-out final evaluation.

## Phase 4: tuning and frozen selection

Sweep the recorded grid using tuning data only, then freeze the verified choice:

```powershell
go run ./cmd/semcache tune -output results/my-tuning
go run ./cmd/semcache freeze-candidate -tuning results/my-tuning -output results/my-final-candidate.json
go run ./cmd/semcache replay -candidate results/my-final-candidate.json -data data/generated/development-101.json -mode end-to-end -start warm -output results/frozen-smoke
```

The sweep requires all declared tuning orders from `build-data` and a new output
directory. It evaluates 1/2/4 tables with 4/8 bits, three workload seeds, three
projection seeds, both replay modes, warm/cold starts and capacities 100/1000.
Controls are always-search, always-skip and exact normalized-vector repeats.
The repeat reference cannot establish semantic membership. Single-bucket recall
was adequate for the 4-table/4-bit family, so nearby probing was unnecessary.

The saved [candidate](configs/final_candidate.json) freezes always-search:
2 tables/4 bits passed 71/72 conditions but failed paired cost once; 4 tables/4
bits achieved perfect recall but failed paired p95 seven times. No family passed
every constraint. Frozen budgets remain 99% recall, 8 MiB summary memory,
1200 ms p95, non-increasing paired p95 and strictly lower paired cost.
These controlled vectors and simulated cost/latency inputs do not establish
production benefit. Final evaluation remains unopened.

`tradeoffs.csv`, `sweep.json`, assessments and per-request evidence record every
condition. Memory includes projections and entry bookkeeping, with independent
retained-heap profiles and separate whole-process working sets. Run microbenchmarks
separately from replays:

```powershell
go test ./internal/semanticcache -run '^$' -bench BenchmarkLSHTuning -benchmem -benchtime 100ms -count 3
```

See the [tuning report](docs/phase4_tuning.md) for measured results, exact
boundaries and selection rationale. `-candidate` includes frozen experiment
settings and all projection seeds; omit `-config` and `-lsh` when using it.
Candidate replays save the exact candidate JSON and its checksum. Freezing refuses
to overwrite an existing candidate file.

The debugging audit revalidated all 504 saved tuning conditions and reproduced
the original selection and assessments. Freezing verifies every row's workload,
configuration, seed, projection identity and paired baseline, and recomputes
metrics from the saved request traces. It rejects incomplete evidence, mismatched
grid or variant metadata, and altered inputs or metrics, even when the changed
files have been rechecksummed. Captured source identities must agree across the
sweep. Verification runs offline during freezing and does not change serving.

Tests, vet, build, race detection, eight development-only fallback replay
conditions and all 108 benchmark samples passed. The original frozen candidate
is preserved, and held-out final evaluation remains unopened.

## Phase 5: lifecycle events and delayed updates

Run the seeded controlled churn experiment:

```powershell
go run ./cmd/semcache churn -config configs/churn.json -output results/my-churn
```

Phase 5.1 adds content-addressed event IDs, contiguous per-stream ordering,
entry-version checks, harmless duplicate/stale deliveries and a reset barrier.
Synchronous cache mutations consume this event stream; maintenance failures keep
search fallback enabled until rebuilding. Six hundred seeded lifecycle mutations
match fresh entry/bucket rebuilds after every step, including exact TTL expiry.

Phase 5.2 simulates delayed, reordered, duplicated and dropped events. Request
and event logs record watermarks, buffered events, event age, reconciliation
state and fallback reasons. Detected gaps, source silence and freshness breaches
disable skipping. A watermarked rebuild replays newer reliable-source events
before enabling the replacement. Prediction reads only local summary state.

The debugging audit's eighteen-run controlled matrix serves 1,080 requests and
restores exact agreement in every run, covering three seeds, fresh/mixed/silent
updates, capacity one, exact TTL boundaries and frequent rebuilds.
Mixed faults record twelve stale-removal wasted searches; silent
tails record nine missed hits before detection. Unknown tail updates cannot be
detected immediately. Structural rebuild memory and sampled process heap peaks
are reported separately, and short Windows timings often round to zero.

The audit fixed source freshness being extended by duplicate/delayed deliveries,
future-dated source data being enabled through recovery, and scheduling evidence
sharing mutable payloads with queued deliveries. Focused regressions reproduced
each failure before its fix. Tests, vet, build and race detection passed.

These fixtures use simulated 2D vectors, logical update delay and fake generation.
The frozen always-search candidate is unchanged. See the
[Phase 5 report](docs/phase5_churn.md) for reproduction settings, event/recovery
contracts, results and measurement limits. The new output directory contains
config/workload/projections, JSONL/CSV traces, metrics, rebuild measurements and
a source/artifact checksum manifest. `churn` accepts `-config` and `-output`.

## Live embeddings

The Go HTTP adapter supports a [Hugging Face TEI server](https://huggingface.co/docs/text-embeddings-inference/quick_tour). Configure the server with:

- Model: `sentence-transformers/all-MiniLM-L6-v2`.
- Revision: `1110a243fdf4706b3f48f1d95db1a4f5529b4d41`.
- Native masked mean pooling, a 256-token limit, and no custom prompt.

With that server listening locally:

```powershell
go run ./cmd/semcache demo -tei http://127.0.0.1:8080 -output results/demo-live
```

The adapter verifies `/info` model identity before each `/embed` request, validates the response dimension and vector norm, limits response size, and propagates cancellation and HTTP timeouts. An unavailable or incompatible server produces an explicit error.

Preprocessing trims surrounding Unicode whitespace, preserves case and internal whitespace, and L2-normalizes vectors. The fixture records the model revision, preprocessing identity, inference origin, and vectors in [demo_embeddings.json](internal/semanticcache/testdata/demo_embeddings.json).

## Tests

```powershell
go test ./... -count=1
go vet ./...
```

Tests cover cache lifecycle and scope isolation, similarity boundaries and ties, malformed embeddings, cancellation, backend/generation/write failures, HTTP protocol behavior, deterministic replay, CLI results, and manifest hashes. LSH tests also cover persisted projections, counted shared buckets, replacement/removal, all 64 hash bits, input ownership, numerical and configuration fallbacks, maintenance failure/rebuild, and oracle sampling/logging invariance.

For race detection on Windows with MSYS2 UCRT64 GCC installed at `C:\msys64`, use these settings in the current PowerShell session:

```powershell
$env:Path = 'C:\msys64\ucrt64\bin;' + $env:Path
$env:CC = 'C:\msys64\ucrt64\bin\gcc.exe'
$env:CGO_ENABLED = '1'
go test -race ./... -count=1
```

Adjust the compiler path for your installation. Tests, vet checks, build, demo, and race-detector tests through Phase 5 passed on Windows/amd64 with Go 1.26.3 and GCC 15.2.0. Phase 4 tests cover tuning boundaries, complete seed matrices, grid and variant metadata, checksum rejection, every row's evidence, request-derived metrics, candidate selection and frozen fallback replay. Phase 5 tests add duplicate/stale event handling, reset barriers, delayed-update attribution, publication-time freshness, future-dated recovery rejection, scheduler payload ownership, and rebuild catch-up with intervening mutations. HTTP integration tests cover the complete adapter path; a Docker TEI deployment was not exercised.

## Measurement boundaries

The offline demo measures routing with precomputed embeddings and excludes original embedding inference. Live mode includes HTTP and embedding inference. Fake generation timings measure deterministic local CPU work, not real LLM latency. Configuration includes simulated cost and latency assumptions for later experiments; the current demo does not apply them.

The demo establishes baseline behavior. It does not establish answer correctness, predictor recall, cost savings, or a performance improvement.

## Repository layout

```text
cmd/semcache/                  CLI and command integration tests
configs/experiment.json        Matching policy and experiment settings
configs/phase2_fixture.json    Controlled evaluation configuration
configs/lsh_development.json   Development LSH projection configuration
configs/tuning_grid.json       Recorded Phase 4 table/bit grid
configs/final_candidate.json   Frozen serving choice and tuning assessments
configs/churn.json             Seeded controlled lifecycle/update-fault experiment

docs/phase4_tuning.md           Tuning results, selection rationale and debugging audit
docs/phase5_churn.md            Event contract, fault recovery and measured boundaries
data/catalog.json              Authored QA groups and split definitions
data/README.md                 Data origin and generation instructions
internal/semanticcache/        Cache, router, adapters, contracts, and tests
internal/semanticcache/testdata/ Recorded real embedding fixture
go.mod                        Go module and toolchain requirement
```

Generated datasets, tuning sweeps, churn matrices, request/event logs, recovery snapshots, replay/debugging evidence, heap/CPU profiles, local projection exports, binaries, coverage files, and the original local planning/progress Markdown files are ignored by Git. Keep experiment outputs under `results/`; recreate them using the commands above. The authored catalog, generator, configurations (including `configs/churn.json`), frozen candidate, regression fixtures, and Phase 4/5 reports are tracked. Raw local evidence referenced by the reports and frozen candidate is not included in the checkout; reproduce churn with the saved configuration and documented seed matrix, and reproduce candidate selection with a new tuning sweep or the retained original run directory.

## Planned work

Phase 5 lifecycle ordering, delayed updates and reconciliation passed the required
implementation, debugging and test checks.
The next phases add remote-cache comparisons and a reproducible final evaluation.
Those components are not implemented yet. The no-qualifying-candidate finding
remains valid work: break-even analysis can continue without claiming savings.
