# Semantic Cache

A Go prototype for reusing answers to semantically similar FAQ requests. Requests are embedded as vectors, matched against a scoped cache, and served from the cache only after the stored answer passes validation. A miss generates a new answer and caches it when eligible.

The research goal is to determine whether a local predictor can skip unproductive semantic-cache searches cheaply enough to reduce total serving cost. **Phases 1 through 7 are implemented within the documented prototype scope**, including final evaluation and operating instructions. No LSH family passed every frozen tuning constraint; the serving candidate uses always-search. Two complete final executions reproduced 216 runs and 82,380 request observations each. The frozen candidate preserved available hits, saved no lookups and made the same generation calls as its baseline. Diagnostic LSH saved searches in miss-heavy stresses but lost hits and added maintenance overhead. Monetary costs and model latencies remain simulated; loopback HTTP/routing time is measured. No production benefit is established.

The Phase 7 debugging audit fixed six evidence/timing-validation gaps and added failing-before/passing-after regressions. Full live tests, build, vet and race checks pass; combined test coverage is 80.1%. Both original final captures pass the stronger verifier, with unchanged results and frozen candidate.

This README contains the setup, results and reproduction commands for a fresh checkout. Detailed reports, result summaries and progress records are kept locally under `docs/`; the entire folder is ignored by Git. Evaluation commands recreate complete evidence under `results/`.

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

Cache access and replay are serialized. The default cache is in memory. The optional Qdrant adapter requires a new, exclusively owned collection for each run.

## Requirements

- Go **1.26.3 or later**.
- A compatible C compiler and cgo for race-detector tests.
- An optional Hugging Face Text Embeddings Inference (TEI) server for live embeddings.
- Optional Qdrant **1.19.2** for remote adapter experiments and live integration tests.

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
real embedding quality or a benefit. Phase 4 tuning is reported below; the
subsequent frozen final evaluation is reported in Phase 7.

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
production benefit. Final evaluation stayed unopened through selection; Phase 7
subsequently evaluated the unchanged frozen candidate.

`tradeoffs.csv`, `sweep.json`, assessments and per-request evidence record every
condition. Memory includes projections and entry bookkeeping, with independent
retained-heap profiles and separate whole-process working sets. Run microbenchmarks
separately from replays:

```powershell
go test ./internal/semanticcache -run '^$' -bench BenchmarkLSHTuning -benchmem -benchtime 100ms -count 3
```

The saved tuning artifacts contain measured results, boundaries and the selection
rationale. `-candidate` includes frozen experiment
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
is preserved. Held-out evaluation remained unopened through that audit and was
subsequently completed in Phase 7.

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
The frozen always-search candidate is unchanged. The new output directory contains
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

Adjust the compiler path for your installation. Tests, vet checks, build, demo, and race-detector tests through Phase 7 passed on Windows/amd64 with Go 1.26.3 and GCC 15.2.0. Phase 4 tests cover tuning boundaries, complete seed matrices, grid and variant metadata, checksum rejection, every row's evidence, request-derived metrics, candidate selection and frozen fallback replay. Phase 5 tests add duplicate/stale event handling, reset barriers, delayed-update attribution, publication-time freshness, future-dated recovery rejection, scheduler payload ownership, and rebuild catch-up with intervening mutations. Phase 6 tests cover live Qdrant/reference parity, explicit backend errors and timeouts, completed-write confirmation, malformed-query rejection, validated cost accounting, CLI evidence hashes and exact offline accounting reproduction. Phase 7 adds stress contracts, delayed recovery, memory guarding, HTTP/CPU accounting, complete matrix evidence verification, clean replay and offline rebuild checks. HTTP integration tests cover the complete adapter path; a Docker TEI deployment was not exercised.

## Measurement boundaries

The offline demo measures routing with precomputed embeddings and excludes original embedding inference. Live mode includes HTTP and embedding inference. Fake generation timings measure deterministic local CPU work, not real LLM latency. Configuration includes simulated cost and latency assumptions for later experiments; the current demo does not apply them.

The demo establishes baseline behavior. It does not establish answer correctness, predictor recall, cost savings, or a performance improvement.

## Phase 6: remote cache experiments

The adapter and final matrix use Qdrant 1.19.2. For a new local Windows backend,
run from the repository root, using a fresh output directory:

```powershell
$backendDir = Join-Path (Get-Location) 'results/qdrant-local'
New-Item -ItemType Directory -Path $backendDir | Out-Null
Invoke-WebRequest -Uri https://github.com/qdrant/qdrant/releases/download/v1.19.2/qdrant-x86_64-pc-windows-msvc.zip -OutFile "$backendDir/qdrant.zip" -UseBasicParsing
Expand-Archive -LiteralPath "$backendDir/qdrant.zip" -DestinationPath $backendDir
@'
log_level: WARN
storage:
  storage_path: ./storage
  snapshots_path: ./snapshots
service:
  host: 127.0.0.1
  http_port: 16333
  grpc_port: 16334
telemetry_disabled: true
'@ | Set-Content "$backendDir/config.yaml" -Encoding UTF8
$qdrantProcess = Start-Process -FilePath "$backendDir/qdrant.exe" -ArgumentList '--config-path','config.yaml' -WorkingDirectory $backendDir -WindowStyle Hidden -PassThru
```

After startup, `Invoke-RestMethod http://127.0.0.1:16333/` must report version
1.19.2. Keep this backend running while executing remote commands. Run full normal
and race suites sequentially so package discovery does not encounter temporary
backend storage being removed by another suite. With Qdrant ready:

```powershell
$env:SEMCACHE_QDRANT_URL = 'http://127.0.0.1:16333'
go test ./... -count=1
go run ./cmd/semcache remote-demo -endpoint http://127.0.0.1:16333 -output results/phase6/my-run -repeats 20 -rebuild-every 8
go run ./cmd/semcache remote-account -run results/phase6/my-run/run.json -config results/phase6/my-run/config.json -output results/phase6/recomputed.json
```

Normal tests run offline when the environment variable is unset. Remote runs save exact source/input bytes, per-request JSONL, HTTP attempts, stage timings, rebuild measurements, CSV/JSON accounting and a checksum manifest. Monetary costs and model latencies are simulated; remote HTTP latency is measured. The exploratory LSH is separate from the frozen always-search candidate.

`remote-demo` accepts `-endpoint`, `-config`, `-output`, `-repeats` and
`-rebuild-every` (zero disables periodic rebuilds). `remote-account` requires
`-run`, `-config` and `-output`, and runs without Qdrant. Use new output paths;
both commands preserve existing evidence. Compare `recomputed.json` with
`accounting.json` byte for byte, and verify the saved manifest hashes.

The Phase 6 debugging audit added strict backend success-response checks and
rejection of invalid accounting measurements, with regression and CLI evidence
tests. Three fresh paired development replays verified 480 requests and exact
offline accounting reproduction. LSH saved two searches per 80 requests while
increasing paired total measured latency; monetary savings remain simulated.

The adapter uses exact search, rescoring original float64 payload vectors after
Qdrant's float32 storage. Scope, TTL/FIFO and answer validation follow the reference
policy. Backend errors fail explicitly. Each run creates and removes its own
collection; it does not reopen existing collections or support external writers.
These loopback measurements do not establish WAN, ANN or production performance.
After all remote tests and replays finish, stop the backend started above with
`Stop-Process -Id $qdrantProcess.Id`.

## Phase 7: final evaluation and handoff

With Go installed, generate the three declared data orders and start the pinned
Qdrant loopback backend using the setup instructions above:

```powershell
go run ./cmd/semcache build-data -seed 101
go run ./cmd/semcache build-data -seed 202
go run ./cmd/semcache build-data -seed 303
go run ./cmd/semcache final-evaluate -output results/my-final
```

The full evaluation covers 21 scenarios: local/remote, both comparison modes,
warm/cold, capacities 100/1000, low/high hits, all-hit/all-miss/empty, churn,
fresh/delayed updates and a filled-cache memory guard. Each condition uses the
frozen candidate and a paired independent baseline. Named LSH stress diagnostics
use the unchanged development configuration across the full three-by-three seed
family; they do not replace the candidate.

The baseline and candidate each served 36,276 requests, made 32,818 searches and
8,449 fake generations, with zero missed hits. Diagnostic stress runs recorded
50 missed hits (44 intrinsic, six unseen updates), six stale-removal searches
and 1,704 fallbacks. Every final summary reconciled; no serving/write/HTTP
failures or backend/reference discrepancies occurred. Routing recall does not
validate answers: original warm final streams incorrectly reused 100 labeled
near-matches per 1,000 requests. The generated `results.csv`, `results.json` and
`aggregates.json` retain each condition's budget violations, cost, timings and
memory, including descriptive variability across seeds.

To reproduce the clean replay check:

```powershell
go run ./cmd/semcache final-evaluate -output results/my-clean-replay
go run ./cmd/semcache final-verify -input results/my-final -replay results/my-clean-replay
```

Every output directory must be new. Verification runs offline and reconstructs
metrics, budgets and tables from complete traces. Both recorded captures reproduce
confusion/call/reuse/fault counts and final states exactly; measured timings,
CPU-priced cost contributions and process memory vary. Verification reconstructs
both captures' complete tables, binds workload/projection identities to the
frozen plan, checks each request's timing boundaries and compares request-level
responses, decisions, updates and fault evidence. It normalizes measured durations
and independently owned collection names.

The original captures used workload seeds 101/202/303 and diagnostic projection
seeds 1001/2002/3003. The frozen budgets are 99% recall when hits exist, 1,200 ms
absolute p95, non-increasing paired p95, strictly lower paired cost and an 8 MiB
summary. Diagnostic recall failed in 27 runs, paired cost in 33 and paired p95 in
45; eighteen diagnostic no-hit runs retain `N/A` recall. All absolute p95/memory
budgets passed. The filled-cache diagnostic summary estimate was 3,641,603 bytes,
independent retained LSH heap 1,816,480-1,816,496 bytes and rebuild overlap estimate
7,283,014 bytes. A 1,024-byte stress guard forced all 108 diagnostic memory-case
decisions to search while retaining the summary. This guard does not replace the
frozen budget. No confidence interval or statistical significance is claimed.

For a small explicit predictor replay and offline summary rebuild:

```powershell
go run ./cmd/semcache build-projections -output results/handoff-projections.json
go run ./cmd/semcache replay -data data/generated/development-101.json -lsh results/handoff-projections.json -start warm -output results/handoff-predictor
$lshRun = Get-ChildItem -Path results/handoff-predictor/lsh-*-run.json | Select-Object -ExpandProperty FullName
go run ./cmd/semcache rebuild -projections results/handoff-projections.json -run $lshRun -output results/handoff-rebuild
```

The rebuild restores TTL/FIFO metadata and verifies the counted summary from the
exact saved projections and snapshot. It writes an offline report and source
inputs. In-process reference serving recovers through `SynchronizedCache.Rebuild()`;
all cache mutations must go through that wrapper. Delayed recovery uses an
authoritative watermarked snapshot with `BeginRebuild` and complete reliable
event catch-up through `FinishRebuild`. Failed recovery keeps search fallback
enabled. An ambiguous remote write requires a new owned collection.

## CLI and configuration reference

Run `go run ./cmd/semcache help` to list commands; each command's `-h` displays
its flags and defaults. Final evaluation, verification and rebuild options are:

| Command | Flags |
| --- | --- |
| `final-evaluate` | `-plan configs/final_matrix.json`, `-candidate configs/final_candidate.json`, `-data-dir data/generated`, `-endpoint http://127.0.0.1:16333`, `-output results/phase7/final` |
| `final-verify` | Required `-input DIR`; optional `-replay DIR` |
| `rebuild` | Required `-projections FILE`, `-run FILE` (LSH replay `*-run.json`), `-output DIR`; `-config configs/phase2_fixture.json` |

Exit codes are 0 for completed execution, 1 for input/operation failure and 2 for
usage errors. A completed matrix can contain budget failures; inspect the tables.
Tuning, churn, remote/final evidence, rebuild and candidate freezing require fresh
output paths. Demo, ordinary replay, data generation and projection export can
overwrite matching output files.

JSON inputs reject unknown fields and trailing values. Authored configurations
are runnable examples; their complete typed schemas and validation rules live
beside the implementation:

| Configuration | Schema and key fields |
| --- | --- |
| Experiment | [Config](internal/semanticcache/config.go): IDs, `policy`, `embedding`, request count, distinct workload/projection seeds, recall/p95/memory budgets, five simulated per-operation `assumptions` |
| LSH | [LSHConfig](internal/semanticcache/lsh.go): version, full embedding identity, seed, `tables` (1-256), `bits` (1-64); persisted projections add actual `planes` |
| Tuning grid | [TuningGrid](internal/semanticcache/tuning.go): ID, table/bit arrays, both comparison modes and starts |
| Frozen candidate | [FinalCandidate](internal/semanticcache/candidate.go): selected router, experiment, embedding/projection identities, tuning assessments, provenance hashes and freeze time |
| Churn | [ChurnConfig](internal/semanticcache/churn.go): seeds, steps/timing, capacity/TTL, freshness/heartbeat/rebuild limits and transport faults |
| Final matrix | [FinalPlan](internal/semanticcache/final.go): ID, exact candidate checksum, diagnostic LSH configuration and named scenarios with backend/mode/start/capacity/stream plus optional request count, fill, update delay, diagnostic and guard settings |
| Workload | [Workload](internal/semanticcache/workload.go): ID/split/seed, origin, embedding identity, groups, warm/empty-cold entries and ordered labeled requests/vectors; see [data generation](data/README.md) |

Every embedding must be finite, nonzero and dimension-compatible. Scope strings
are case-sensitive and timestamps use UTC. Undefined output metrics are `"N/A"`.
`replay -candidate` supplies frozen configuration/projections; omit explicit
`-config` and `-lsh`. Original final streams use the entire declared split;
separately named stress streams contain 12-1,000 requests. Final evaluation never
changes the candidate or frozen budgets.

## Repository layout

```text
cmd/semcache/                  CLI and command integration tests
configs/experiment.json        Matching policy and experiment settings
configs/phase2_fixture.json    Controlled evaluation configuration
configs/lsh_development.json   Development LSH projection configuration
configs/tuning_grid.json       Recorded Phase 4 table/bit grid
configs/final_candidate.json   Frozen serving choice and tuning assessments
configs/churn.json             Seeded controlled lifecycle/update-fault experiment
configs/final_matrix.json      Predeclared final evaluation/stress conditions

docs/                         Local documentation/reports/progress (ignored)
results/                      Generated evidence, profiles and backend state (ignored)
data/catalog.json              Authored QA groups and split definitions
data/README.md                 Data origin and generation instructions
internal/semanticcache/        Cache, router, adapters, contracts, and tests
internal/semanticcache/testdata/ Recorded real embedding fixture
go.mod                        Go module and toolchain requirement
```

The entire `docs/` folder is local and ignored, including reports, progress and
summary CSV/JSON. Previously tracked phase reports are removed from the Git index
while their local files are preserved. Generated datasets, experiment evidence,
profiles, local projections, binaries, planning files and backend state are also
ignored. Keep generated artifacts and downloaded Qdrant files under `results/`;
root Qdrant `storage/` and `snapshots/` are ignored as well. Source, tests, authored
catalog/configurations, frozen candidate and this README remain tracked. Recreate
the full result tables and traces with the documented evaluation commands.

## Scope and follow-up

Required prototype deliverables are complete. The no-qualifying-LSH finding
remains unchanged. These results motivate follow-up experiments with real held-out
embeddings, stronger answer-validity calibration, bounded probes and dependable
update delivery. WAN/production savings remain unestablished.
