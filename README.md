# Semantic Cache

A Go prototype for reusing answers to semantically similar FAQ requests. Requests are embedded as vectors, matched against a scoped cache, and served from the cache only after the stored answer passes validation. A miss generates a new answer and caches it when eligible.

The research goal is to determine whether a local predictor can skip unproductive semantic-cache searches cheaply enough to reduce total serving cost. **Phases 1 and 2 are implemented:** the baseline, validated synthetic workloads, read-only oracle, paired replay modes, and checked metrics. The LSH predictor is planned for Phase 3.

## How it works

```text
Request -> embed -> search eligible entries -> validate match
                                              | valid: return cached answer
                                              | miss/rejected: generate -> cache if eligible
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

Tests cover cache lifecycle and scope isolation, similarity boundaries and ties, malformed embeddings, cancellation, backend/generation/write failures, HTTP protocol behavior, deterministic replay, CLI results, and manifest hashes.

For race detection on Windows with MSYS2 UCRT64 GCC installed at `C:\msys64`, use these settings in the current PowerShell session:

```powershell
$env:Path = 'C:\msys64\ucrt64\bin;' + $env:Path
$env:CC = 'C:\msys64\ucrt64\bin\gcc.exe'
$env:CGO_ENABLED = '1'
go test -race ./... -count=1
```

Adjust the compiler path for your installation. The Phase 1 tests, vet checks, build, demo, and race-detector tests passed on Windows/amd64 with Go 1.26.3 and GCC 15.2.0. HTTP integration tests cover the complete adapter path; a Docker TEI deployment was not exercised.

## Measurement boundaries

The offline demo measures routing with precomputed embeddings and excludes original embedding inference. Live mode includes HTTP and embedding inference. Fake generation timings measure deterministic local CPU work, not real LLM latency. Configuration includes simulated cost and latency assumptions for later experiments; the current demo does not apply them.

The demo establishes baseline behavior. It does not establish answer correctness, predictor recall, cost savings, or a performance improvement.

## Repository layout

```text
cmd/semcache/                  CLI and command integration tests
configs/experiment.json        Matching policy and experiment settings
internal/semanticcache/        Cache, router, adapters, contracts, and tests
internal/semanticcache/testdata/ Recorded real embedding fixture
go.mod                        Go module and toolchain requirement
```

Generated datasets, runs, binaries, coverage files, and the original local planning/progress Markdown files are ignored by Git. The authored catalog, generator, configuration, and public documentation are tracked.

## Planned work

The next phase adds an LSH bucket-occupancy predictor. Later phases cover lifecycle synchronization, recall/memory/cost tuning, remote-cache comparisons, and a reproducible final evaluation. These components are not implemented yet.
