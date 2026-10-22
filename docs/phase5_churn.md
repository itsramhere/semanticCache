# Phase 5: lifecycle events and stale-summary recovery

Phase 5.1 was implemented and verified before Phase 5.2. The frozen
`configs/final_candidate.json` still selects always-search. These experiments
exercise recovery with a separate controlled LSH fixture; they do not select
a new candidate or open held-out final evaluation.

## Run the experiment

From the repository root, with Go 1.26.3:

```powershell
go run ./cmd/semcache churn -config configs/churn.json -output results/my-churn
go test ./... -count=1
go vet ./...
```

The output directory must be new. The command saves the complete typed
configuration, authored/generated workload, initial/final cache snapshots,
projection coefficients, request/event JSONL, request CSV, metrics, rebuild
measurements, and a source/artifact checksum manifest. It records the Go
version, Git revision/status, source hashes, command and independent workload,
projection and transport seeds. Generated results remain ignored by Git.

The manually authored workload repeats a twelve-step sequence: insertion,
requests, repeated deletion, replacement, revocation, eviction, reset, and
another insertion. Expiration runs before each mutation/request; serving writes
also exercise capacity eviction. The workload seed varies positive/negative
queries. The fixture uses simulated normalized 2D vectors and the deterministic
fake LLM. Default capacity is three entries, TTL five logical seconds, threshold
0.90, two tables/four bits, and projection seed 1001. These lifecycle stress
settings are separate from the frozen experiment configuration.

## Lifecycle contract (5.1)

`UpdateJournal` captures the serialized owner's committed state. Within one
mutation it emits removals in entry-ID order, then changed/new entries in
entry-ID order. A replacement advances the entry version even when its answer,
embedding and timestamp are unchanged. Repeated deletion emits no update.
Each event has a stream ID, monotonically increasing sequence, logical source
timestamp, kind, previous entry version and SHA-256 ID of its complete payload.
Upserts assign their sequence as the new entry version. FIFO insertion order is
independent of update ordering.

`EventSummary` accepts contiguous events and checks previous versions before
changing bucket contributions. Duplicate/older deliveries are harmless.
Wrong-incarnation removals and sequence gaps disable skipping. A reset is an
ordered barrier; old deliveries cannot resurrect entries. `Clear` never reuses
FIFO insertion versions. Counts remain unsigned and zero-count buckets are
removed. Snapshot and event vectors are owned copies.

`SynchronizedCache` is the synchronous serving owner. Its event journal retains
only the latest state and version map, rather than an unbounded history.
Mutation or retrieval through a retained underlying cache reference is
unsupported. A committed cache write remains successful when summary maintenance
fails, with an explicit maintenance error and search fallback until rebuild.
Its reported memory includes the journal snapshot and both version maps.
Isolation replay continues to use the authoritative snapshot stream.

Stream IDs identify independent update streams; sequences are local to that
stream. Reusing a stream ID across independent restarts is unsupported. A
transport restart must retain the watermarked source state/journal, or start a
new stream and rebuild. This is a serialized prototype, without a durable
distributed log or concurrent mutation API.

Tests compare both entry ledgers and bucket counts against a fresh rebuild
after 600 mutations across seeds 101/202/303. They cover shared buckets,
replacement, repeated deletion, expiration, capacity/explicit eviction,
revocation and full reset. Separate regressions cover exact expiration at the
request timestamp, duplicate delivery after replacement/reset, stable event IDs,
and stale entry versions without decrementing current contributions.

## Faults and observability (5.2)

`EventScheduler` uses a separate seeded RNG. It assigns a bounded logical delay
to each event, drops selected events, and optionally schedules a duplicate with
its own delay. Deliveries sort by due time and then scheduling order. Different
delays produce reordering. No simulated delay is slept.

`DelayedSummary` predicts from local data only. Its logs distinguish:

- Last applied sequence and the highest sequence observed from deliveries or
  a source heartbeat.
- Received buffered events and the number of known unapplied source sequences.
- A lower bound on the oldest known unapplied-event age and time since the last
  evidenced source activity. An event contributes its source publication time;
  a current heartbeat contributes its observation time. Unknown age is JSON
  `null`, rather than a fabricated zero.
- Readiness, reconciliation state and the first latched fallback reason.

A delivered sequence gap, heartbeat watermark ahead of applied state, invalid
event, or freshness-limit breach immediately disables skipping. Silence reaches
the freshness limit at the inclusive boundary. A late contact does not erase a
breach that has already occurred. Buffered events drain in contiguous order, but
fallback stays latched until a verified rebuild. During rebuilding, all
predictions search.

Delayed or duplicate deliveries cannot refresh source activity to their receipt
time. Otherwise, a transport replaying an old event could indefinitely conceal
source silence after losing a final insertion. Heartbeats remain the explicit
way to attest that a source with no new mutations is current.

A dropped final insertion cannot be identified immediately without a newer
delivery or heartbeat. Before the freshness limit, its bucket may be unseen and
a real hit may be skipped. A dropped final removal can leave an occupied bucket
and cause a wasted search. Heartbeats help detect these conditions; they do not
establish continuous freshness between contacts. Receiver metrics cannot know
all undelivered events. The experiment therefore logs separate **diagnostic**
actual backlog/oldest age from its authoritative history and transport queue;
prediction does not use those fields.

Each request also gets a read-only exact-cache oracle and a fresh-summary
counterfactual with identical projections. An actual skipped hit that the fresh
summary would search is an unseen-upsert miss. An occupied-bucket search on an
oracle miss that the fresh summary would skip is a stale-removal search
(including replacement, revocation, expiration, eviction and reset removals).
Intrinsic LSH misses and fallback miss searches are separate. Oracle/fresh-summary
work does not alter serving or enter request timing.

## Watermarked rebuilding

`BeginRebuild` builds a replacement from an owned source snapshot with stream,
embedding identity, entry versions and event watermark. The old predictor stays
in fallback. Source mutations can continue between Begin and Finish in the
serialized replay. `FinishRebuild` replays a reliable journal of newer events
through the current source watermark, then swaps the active consumer pointer.
Missing events, incompatible identity, stale watermarks or failed hashing leave
the replacement disabled. This assumes a reliable source journal/snapshot for
recovery; a second pass through the faulty delivery queue is insufficient.
Initialization rejects snapshots newer than the receiving clock. Catch-up cannot
enable a snapshot or newer event dated after its completion time; a retry at the
actual source time can complete recovery.

Tests cover writes, removal and reset during rebuilding, incomplete catch-up
without a swap, wrong model revision, malformed events, exact freshness limits,
and successful restoration of complete bucket/entry/version agreement. No
goroutines are needed: the Begin/Finish boundary deterministically exercises
mutations during a rebuild, and the final swap is atomic with respect to this
serialized owner. Callers must serialize these APIs.

## Saved evidence

The debugging-audit capture is `results/phase5-debugging/matrix-v2`, reproduced
by `results/phase5-debugging/verify.ps1` using the built audit CLI. Eighteen runs
serve 1,080 requests. For workload seeds
101/202/303 use transport seeds 5001/5002/5003 respectively. Each seed pair runs
these six configurations, otherwise retaining `configs/churn.json`:

| Scenario | Maximum delay | Drop probability | Duplicate probability | Heartbeat interval |
| --- | --- | --- | --- | --- |
| Fresh | 0 ms | 0 | 0 | Every step |
| Mixed | 6000 ms | 0.15 | 0.30 | Every four steps |
| Silent tail | 0 ms | 1 | 0 | Disabled |
| Capacity one | 6000 ms | 0.15 | 0.30 | Every four steps |
| TTL boundary | 0 ms | 0 | 0 | Every step |
| Frequent rebuild | 6000 ms | 0.15 | 0.30 | Disabled |

All use 60 steps and 3000 ms freshness. Capacity one sets capacity to one;
TTL boundary sets TTL to one second, matching the one-second step exactly.
Frequent rebuild uses 3000 ms steps and rebuilds every two steps; other cases
use 1000 ms steps and rebuild every twelve steps. Rebuilds finish on the
following step after source mutations. A final rebuild verifies agreement.
The capture records source revision
`f4f9da65f770910c810c8e880082169f9cbb2cbc` plus modified source digest
`99d5045b24748597c911f40a44321fc5d364156d0bbf0272e83969063410c214`.
The earlier `results/phase5/matrix-v3` capture remains historical; the audit
replaces its source and memory evidence.

Aggregated across the three seeds (180 requests per scenario):

| Scenario | Searches | Fake LLM calls | Unseen-upsert missed hits | Stale-removal wasted searches | Fallback requests |
| --- | ---: | ---: | ---: | ---: | ---: |
| Fresh | 125 | 55 | 0 | 0 | 12 |
| Mixed | 180 | 55 | 0 | 12 | 148 |
| Silent tail | 171 | 61 | 9 | 12 | 135 |
| Capacity one | 178 | 92 | 2 | 12 | 146 |
| TTL boundary | 38 | 142 | 0 | 0 | 12 |
| Frequent rebuild | 140 | 95 | 3 | 0 | 90 |

All eighteen runs finish in exact agreement, with no serving/write failures. No
intrinsic LSH misses occur with these two opposite vectors. Four planned-rebuild
requests per fresh run explain its fallback count. Mixed runs lose lookup
savings while fallback protects recall. Silent-tail runs expose three unseen
misses per seed before detection. Nine missed hits produce six additional
generations relative to the fresh runs; diverging cache states prevent treating
each false negative as exactly one additional LLM call. These counts establish
failure/recovery behavior, without a production benefit claim.

## Measurement boundaries and checks

The audit matrix measures instrumented local maintenance at 1.0953–5.9753 ms per
60-request run, including final reconciliation. Default Windows clock resolution
rounds many individual stages to zero; aggregate predictor time ranges from
zero to 547,900 ns. Rebuild build/catch-up values are likewise
often zero. These zeros do not establish zero CPU cost or support a latency
comparison. No real embedding inference, network delay, LLM latency or monetary
cost is measured; vectors, workload time/delay and generation behavior are
explicitly simulated.

Active predictor structural estimates peak at 5,821–16,934 bytes. Rebuild estimates
peak at 9,047–23,681 bytes, including active/replacement summaries, version maps
and key strings, rebuild metadata, received-buffer map high-water capacity,
snapshot and catch-up events. Deleting
buffered entries does not erase retained map-capacity allowances; a regression
checks this. Source cache/journal and diagnostic history are separate from the
delayed local predictor estimate. Synchronous replay includes its owner journal.

Process `HeapAlloc` samples taken after build and each catch-up event peak at
1,577,464–2,112,440 bytes, including the harness and logging allocations. This is
a **sampled lower bound** on process heap peak, not a continuously observed
maximum, retained-summary measurement, or OS working set. Rebuild timing records
build and catch-up separately; source snapshot extraction is charged to total
maintenance. Catch-up includes heap-sampling overhead. Actual source backlog
peaks at twenty-six events, while these short rebuilds need at most four newer
events to catch up. Larger/concurrent workload performance remains unmeasured.

Verified all 180 artifact checksums and 1,260 captured source-file checksums.
Recomputed request counts, confusion, attribution, generations, fallback and
timing totals from saved traces. Required
checks passed: `go test ./... -count=1`, `go vet ./...`, build, gofmt and diff
checks, plus the documented Windows MSYS2 `go test -race ./... -count=1`.
Tests also cover CLI output/overwrite refusal, malformed configuration,
deterministic transport and replay apart from measurements, cancellation,
and attribution/accounting. The candidate SHA-256 remains
`2fa91e79e6a1238445758db643dfa3bb3611c5e396ec7e8628e74acd8801b060`.

## Debugging audit

The audit reproduced three defects with failing regressions before fixing them:

1. Duplicate/delayed event receipt advanced the source-silence timer to receipt
   time, concealing lost tail updates. Freshness now advances using publication
   time or a current heartbeat. Tests cover duplicate replay, delayed publication
   and the inclusive freshness boundary.
2. Recovery accepted future-dated snapshots and catch-up events even though normal
   delivery rejected future events. Initialization and rebuild completion now
   reject those states; tests verify no swap and successful retry at source time.
3. Returned scheduler evidence shared entry/vector pointers with queued delivery
   payloads. The queue now owns independent copies. A regression mutates both
   source events and returned original/duplicate evidence without corrupting
   eventual delivery or content-addressed IDs.

Rebuild structural accounting also now includes the rebuild object and replacement
version-map key strings. Before/after regressions, coverage, vet, build, race
output, matrices and smoke checks are saved under `results/phase5-debugging`.
The initial audit `matrix` directory is an incomplete capture interrupted by a
PowerShell JSON-array wrapping error in the verification script; `matrix-v2`
is the complete capture. The script was corrected without changing Go behavior.

All tests passed on Go 1.26.3 windows/amd64, including MSYS2 GCC 15.2.0 race
detection. Eight development-only replay conditions (both modes, warm/cold,
capacities 100/1000) passed: 24 router runs and 2,400 requests, with all 88
artifacts verified and no serving/write failures. The baseline demo, fourteen
policy examples and frozen always-search development replay passed. Held-out
final evaluation remains unopened.
