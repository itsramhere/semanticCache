# Phase 2 evaluation data

`catalog.json` is the source of `synthetic-helpdesk-v1`: 50 manually authored
fictional AcmeDesk answer groups, five per topic. No private or user data is used.
Development topics are password reset and sign-in; tuning topics are invitations,
notifications, profile and search; final topics are export, cancellation, storage
and recovery. Topic and group assignments are fixed before router experiments.

Generate and validate the saved orders and initial states:

```powershell
go run ./cmd/semcache build-data -seed 101
go run ./cmd/semcache build-data -seed 202
go run ./cmd/semcache build-data -seed 303
go run ./cmd/semcache validate-data data/generated/development-101.json data/generated/tuning-101.json data/generated/final-101.json
```

Each seed has 100 development requests and 1,000 requests in each other split.
Primary categories are disjoint: 20% exact repeats, 30% paraphrases, 20% unrelated
training questions, 10% administrator near-matches, 10% scope conflicts and 10%
personal noncacheable questions. All six scope fields appear among conflicts.
Exact repeats share canonical text. Paraphrases share an authored answer class.
Training and administrator questions have distinct answer classes. Reusability
labels are separate from computed vector outcomes; scope equality is also
required to reuse an answer. Personal questions cannot be reused or inserted.

Warm state contains one canonical answer per group with preserved timestamps.
Cold state is explicitly empty. Request order is shuffled with an isolated Go
PRNG and the recorded seed; timestamps increase by one second from 00:01 UTC
on 2026-10-21. Generation is deterministic local fake-LLM behavior. No router
results are used to construct or label data.

## Geometry and limits

Phase 2 uses **simulated controlled vectors**, not recorded MiniLM embeddings.
Model identity is `simulated-faq-geometry`, revision `v1`, dimension 64.
The matching policy remains frozen. `configs/phase2_fixture.json` explicitly
selects this fixture; `configs/experiment.json` still selects real MiniLM.
Each group has an orthogonal axis. Paraphrases have canonical cosine 0.98,
administrator near-matches 0.96 and unrelated training questions -1. These
are constructed inputs, not measured semantic similarities. In particular,
administrator questions can match the vector policy while their answer labels
reject reuse. This deliberately tests separate reporting of routing quality
and answer correctness. Repeated synthetic templates are a limitation.

Generated JSON is reproducible and ignored by Git; the authored catalog and
generator are tracked. Replay saves its exact selected workload and hashes.
Final split files may undergo automated integrity checks; do not replay or
inspect their outcomes before freezing a candidate. These fixtures validate
the harness, and cannot establish real embedding recall or production benefit.

## Representative manual review, 2026-10-21

Review performed on authored development/tuning templates and labels. These
are human-readable reasoning checks by the implementing agent, not a claim
of independent human annotation or review of held-out vector outcomes.

| Group / category | Review of question and label |
| --- | --- |
| password-reset-1 / exact | Requesting a reset uses Forgot password; canonical answer applies. |
| password-reset-1 / paraphrase | Asking where the steps are has the same answer class. |
| password-reset-1 / near-match | An admin resetting another user's password requires a different answer. |
| password-reset-1 / unrelated | Availability of training is not answered by reset steps. |
| sign-in-2 / exact | Recovery-code instructions answer recovery of two-factor access. |
| sign-in-2 / noncacheable | Current personal status has no reusable FAQ answer. |
| invitations-1 / paraphrase | Steps to invite a member reuse the invite instructions. |
| invitations-2 / exact | Revoking differs from resending; group IDs remain separate. |
| notifications-1 / scope conflict | A different tenant cannot reuse the canonical scoped entry. |
| notifications-3 / near-match | Quiet hours for another user require admin-specific advice. |
| profile-1 / noncacheable | Personal current status remains ineligible despite a matching vector. |
| search-1 / unrelated | A training-course question is distinct from exact-phrase syntax. |

The fake LLM is not a QA model; correctness means whether a retrieved answer's
authored class and scope apply, not whether generated text is factual.
