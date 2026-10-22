# Phase 6: remote cache experiments

## Backend and adapter (6.1)

Qdrant 1.19.2 (`016542aa5deb6c66380bb137badf73d54f742bde`), Go standard-library HTTP adapter v1 (no added Go dependencies). Official documentation consulted on 2026-10-22:

- https://qdrant.tech/documentation/installation/
- https://qdrant.tech/documentation/search/
- https://qdrant.tech/documentation/search/filtering/
- https://api.qdrant.tech/api-reference/search/query-points
- https://api.qdrant.tech/api-reference/points/upsert-points
- https://api.qdrant.tech/api-reference/points/delete-points
- https://github.com/qdrant/qdrant/releases/tag/v1.19.2

The adapter creates a new collection and requires exclusive sequential ownership. It does not reopen an existing collection or support external writers. Scope is the canonical JSON encoding of every reference scope field. The local ledger assigns reference TTL/FIFO metadata; serving searches use HTTP results. Logical timestamps provide inclusive expiration, independent of server wall time. Expired entries remain conservatively represented in the summary until the next write; they can cause extra searches, never false negatives.

Vectors are normalized in float64 and converted to float32 for Qdrant Cosine storage. Qdrant returns cosine similarity directly (higher is better); no `1-distance` conversion applies. Search uses `exact:true`, no remote score cutoff, and a candidate limit equal to the owned collection capacity. All returned candidates are rescored with the original float64 vector, inclusive threshold, answer validity and deterministic reference tie-breaking. Original entries are stored as JSON strings because numeric payload reserialization changed float64 values during a real 384-dimensional development smoke test. Full scoped candidate transfer is an explicit correctness/performance tradeoff, not an ANN performance claim. Approximate search is disabled; observed backend/reference discrepancies and LSH skipped hits are reported separately.

Writes validate/stage before HTTP, await acknowledgements, remove expired/evicted points, then commit the local ledger and maintain the summary. Partial or ambiguous write failures latch backend unavailability and summary fallback; recovery requires a new owned collection. A pre-cancelled write changes no state. Search errors fail the request with an explicit error and do not generate/insert as though the cache missed. Both routers share these rules. Each HTTP attempt has its method, path, duration and error recorded. Prediction reads only the local summary; parity tests assert zero HTTP calls during prediction.

### Reproduce on Windows

For a fresh local binary setup (run from the repository root):

```powershell
$backendDir = Join-Path (Get-Location) 'results/phase6/backend-new'
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
Invoke-RestMethod http://127.0.0.1:16333/
# After all tests/replays finish: Stop-Process -Id $qdrantProcess.Id
```

Wait for the readiness request to return the expected backend version before testing. The verification service used for this report was stopped after all captures; its config remains at `results/phase6/backend/qdrant/config.yaml` (SHA256 `a6a5b53868215117827d5d17bfe53a981c6164bde1698bbba519442d8c32d047`). Windows filesystem-type and missing web-UI warnings did not prevent the API tests. No production storage-durability claim is made.

Download the official `qdrant-x86_64-pc-windows-msvc.zip` from the pinned release into an ignored results directory and extract it. Run the binary with a YAML config containing `service.host: 127.0.0.1`, HTTP port 16333, gRPC port 16334, and storage/snapshot directories underneath that results directory. Set `telemetry_disabled: true`. Start from that directory; keep it running while executing:

```powershell
$env:SEMCACHE_QDRANT_URL = 'http://127.0.0.1:16333'
go test ./... -count=1
go run ./cmd/semcache remote-demo -endpoint http://127.0.0.1:16333 -output results/phase6/my-demo -repeats 1
```

Tests normally run offline; the environment variable enables actual backend parity tests. Each live test/demo creates a unique collection and deletes only that collection afterward. A failed cleanup is reported. The downloaded backend is a local development service, not a deployed remote host: latency results must be labelled loopback HTTP. `remote-demo` uses the four existing development requests with precomputed real MiniLM embeddings and deterministic fake generation. The exploratory LSH (seed 1001, 2 tables, 3 bits) is not the frozen candidate, which remains always-search. Final evaluation data are unopened.

## Cost and latency accounting (6.2)

`remote-demo` also accepts `-config configs/experiment.json` for saved cost/model-latency assumptions and `-rebuild-every 8` (zero disables rebuilding). It saves raw paired traces, per-router JSONL, a CSV summary, backend identity, source snapshots and SHA256 manifest. Output directories must be new. For an 80-request development pair with ten synchronous summary rebuilds:

```powershell
go build -o results/phase6/semcache.exe ./cmd/semcache
& ./results/phase6/semcache.exe remote-demo -endpoint http://127.0.0.1:16333 -output results/phase6/my-accounting -repeats 20 -rebuild-every 8
& ./results/phase6/semcache.exe remote-account -run results/phase6/my-accounting/run.json -config results/phase6/my-accounting/config.json -output results/phase6/recomputed.json
```

`remote-account` is offline. Its JSON must match the original `accounting.json` byte for byte. It rejects different paired requests and inconsistent search/HTTP counters. Recomputed arithmetic does not authenticate inputs; verify their saved manifest hashes too.

Measured stages include precomputed-vector retrieval/normalization, prediction, remote search, answer validation, fake generation, cache writes, summary maintenance and complete routing. Diagnostics are excluded. Initial build and periodic rebuild durations are charged once in run totals; each synchronous rebuild also contributes to its triggering request's latency quantiles. Initial build is amortized outside per-request tails. Collection creation/deletion latency is setup/cleanup, outside serving latency. All HTTP attempts, failures included, are recorded separately from logical operations.

Money is simulated throughout. Total cost is embedding attempts times the assumed embedding price, generation attempts times the same assumed generation price in both paths, actual search HTTP attempts times the assumed lookup price, all write/admin HTTP attempts times the assumed write price, plus a measured local wall-clock residual priced at the simulated per-millisecond CPU rate. The residual removes HTTP time and the embedding/fake-generation stages from routing time and adds build/rebuild time. It is a proxy that includes scheduling, not a profiler measurement of CPU consumption. Backend CPU/network billing is represented only by the per-operation assumed prices; no cloud invoice or production price is measured. Maintenance is already in measured routing residual and is never added twice.

Measured routing p50/p95 and mixed end-to-end p50/p95 are separate. Mixed estimates replace precomputed-embedding and fake-generation durations with the saved simulated model latencies; actual remote search/write times remain measured. The config's simulated search/write delays are not added to measured HTTP. No network delay is injected. Embeddings are always computed/retrieved even on bypass. Extra generation is charged using observed paired counts, never FN count alone.

Break-even cost solves `predictor nonsearch cost - baseline nonsearch cost - saved searches * lookup price = 0`. Break-even latency uses paired total mixed latency with observed search stages removed, then substitutes a uniform simulated lookup latency. It concerns total time, not p95. A zero/nonpositive saved-search denominator gives N/A; a negative threshold means benefit throughout the nonnegative input range. The saved sensitivity grid shows both signs where a crossing exists. This is a conditional model, not a measured network experiment.

Memory reports separate the active summary's structural estimate, estimated old/new/snapshot rebuild overlap, and sampled whole-process HeapAlloc. Samples include HTTP/harness/runtime allocations and are lower bounds on heap peaks, not retained-summary size, continuous peaks or working set. The shared reference ledger is required by this single-owner adapter in both routers; it is not included in the incremental summary-size column. The overlap estimate includes a reference-state-sized snapshot allowance. No concurrent serving is performed during rebuild.

This small, repeated development stream does not test domain generalization, WAN latency, ANN search, large-cache scaling or a real generation service. The exploratory LSH has not qualified under frozen tuning budgets. The final candidate remains always-search, and Phase 7 remains separate.

## Verified development results

`results/phase6/verified-matrix` contains three independently timed repetitions of the same fixed, ordered development workload (80 requests/router, 480 requests overall). Projection seed 1001, two tables, three bits, capacity 100, reference policy v1, cold start and rebuild every eight requests are fixed. This is measurement repetition, not three independent workload/projection seeds or a Phase 7 evaluation. Baseline runs first in each pair; order/system-load effects remain possible. No confidence interval or statistical significance is claimed.

| Per 80 requests | Always-search | Exploratory LSH |
| --- | ---: | ---: |
| Searches | 80 | 78 |
| Fake generation calls / write HTTP calls | 3 / 3 | 3 / 3 |
| Admin HTTP calls | 2 | 2 |
| Recall on own-state oracle labels | 1 | 1 |
| Lookup reduction | 0 | 0.025 |
| Measured routing p95, milliseconds, observed range | 3.6045–4.3004 | 4.2299–4.5446 |
| Mixed end-to-end p95, milliseconds, observed range | 8.6045–9.3004 | 9.2299–9.5446 |
| Simulated total cost USD, approximate | 0.0149004 | 0.0147005 |
| Structural summary bytes | 0 | 40,246 |
| Estimated rebuild overlap bytes | 0 | 91,909 |

All runs have zero serving/write/HTTP failures and zero remote/reference discrepancies. LSH misses no hits on this tiny repeated stream; this does not override the Phase 4 tuning failure or imply semantic recall guarantees. Two initial misses are bypassed; most later requests hit, so sustained savings are small.

LSH increases paired total measured routing time by 20.2485–26.0614 ms and mixed estimated time by 20.2485–27.6631 ms. Model call counts cancel; differences in measured stub durations explain the slightly different ranges. Ten rebuilds per predictor run total 11.8522–14.9379 ms. Build, updates and rebuilds amortize to 172.554–199.960 microseconds/request. Sampled process heap peaks at rebuild boundaries are 2,657,032–3,144,136 bytes, with the limitations above. Active summary memory is a structural estimate, not those process samples.

Under the saved assumed search price of USD 0.0001/call, paired simulated cost falls by approximately USD 0.00019984–0.00019989. Simulated lookup-price break-even is USD 5.38805e-8–7.88425e-8/call under the very small assumed local CPU price. Uniform lookup-latency break-even for paired total time is 7.6430–8.58315 ms. Measured loopback operation times are below that range and the measured paired totals regress. These are conditional arithmetic thresholds, not WAN measurements or evidence of cloud billing savings. The sensitivity rows include losing and winning regions.

`results/phase6/matrix-verification.txt` records verification of 228 artifact checksums, every captured current source file, HTTP counters, three byte-identical offline accounting reproductions and unchanged frozen-candidate SHA256. Each manifest records the starting Git revision plus modified source bytes, command, toolchain, client and actual backend version. Source snapshots include the real recorded embeddings, without traversing held-out datasets. Earlier adapter-smoke (failed numeric-payload transport), accounting-development and accounting-v2 outputs are development evidence, superseded for final measurements by verified-matrix.

Validation: Go 1.26.3 windows/amd64; final go test ./... -count=1 passed (CLI 27.630s, library 5.750s), go vet ./... passed, go test -race ./... -count=1 passed (CLI 46.190s, library 17.516s) using process-local MSYS2 UCRT64 GCC/cgo. Live parity tests verify backend cosine scores 1, 0, -1 and 0.9 within float32 precision, float64 threshold/tie/scoping/lifecycle agreement, actual 384D transport and rebuilding. Injected tests verify deadline, cancellation, malformed response, backend outage behavior in both public routers, observed-generation accounting, undefined denominators, paired-input/counter rejection and hand-computed break-even/rebuild totals. No application goroutines were introduced.

## Phase 6 debugging audit, 2026-10-22

The follow-up audit reproduced 28 failing regression cases before the fixes
(`results/phase6/debug-audit/regressions-before.txt`). Collection creation could
accept `result:false` or `null`; point writes and eviction deletes could commit
the local ledger without a completed operation; a query with a missing or null
points array could become an ordinary miss. The HTTP adapter now requires true
collection confirmation, `status:completed` for its `wait=true` mutations, and an
actual points array for queries. Failed mutation confirmation preserves the old
ledger and disables skipping. Protocol errors appear in the HTTP attempt log.
An empty points array remains a valid miss. The live pinned backend passes these
stricter checks.

Accounting also accepted negative operation counts, stage/admin durations and
memory measurements, duplicate request IDs and an unknown paired router. These
inputs now fail validation, along with impossible stage totals and operation
counts. Valid saved arithmetic is unchanged. New CLI integration tests exercise
offline accounting, live replay and rebuilding, byte-identical recomputation,
manifest artifact hashes and refusal to overwrite existing evidence.

The current-source measurement capture is
`results/phase6/debug-audit/matrix/summary.csv`. It supersedes the earlier matrix
for timings of the modified adapter; the earlier artifacts are preserved and
still reproduce their original accounting. The same baseline-first, fixed-seed,
80-request development pair was timed three times after tests finished:

| Per 80 requests, observed range | Always-search | Exploratory LSH |
| --- | ---: | ---: |
| Searches | 80 | 78 |
| Generations / write HTTP calls | 3 / 3 | 3 / 3 |
| Measured routing p95, ms | 3.9779-5.1229 | 4.3936-4.5440 |
| Mixed end-to-end p95, ms | 8.9779-10.1229 | 9.3936-9.5440 |
| Simulated total cost USD, approximate | 0.0149003-0.0149004 | 0.0147004-0.0147005 |
| Structural summary / rebuild overlap bytes | 0 / 0 | 40,246 / 91,909 |

All 480 requests have zero serving/write/HTTP errors and zero reference
discrepancies. Own-state hit recall is 1 on this small stream and LSH lookup
reduction is 0.025. LSH increases total measured routing time in each pair by
4.1114-33.1779 ms; p95 increases in two pairs and decreases in one. Mixed total
latency increases by 4.0953-32.1149 ms. Simulated cost decreases by
USD 0.0001998871-0.0001999479 under the saved assumptions. Conditional lookup
price break-even is USD 2.6064e-8-5.6471e-8 per call and uniform lookup-latency
break-even for paired total time is 6.2724-8.96205 ms. These results retain the
measurement limits above and establish no production or WAN benefit.

The audit verified 234 new artifact hashes, every captured current Go source,
HTTP counters and three byte-identical offline accounting reproductions. It
also verified all 228 original artifact hashes and three original accounting
reproductions. Evidence and the reproduction script are under
`results/phase6/debug-audit/`. Live full tests passed (CLI 24.302s, library
5.262s), vet and build passed, and live race tests passed (CLI 47.610s, library
17.056s). Go formatting and `git diff --check` passed. The frozen always-search
candidate is unchanged and Phase 7 evaluation remains unopened.

The final offline suite also passed with the backend variable unset (CLI
22.348s, library 4.312s). Cleanup verified zero remaining collections and stopped
the task-owned backend after checking its executable path.
