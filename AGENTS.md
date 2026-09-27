# AGENTS.md

## Agent skills

### Issue tracker

Issues are tracked in GitHub Issues on `gutrovalim/akaiito-application-test` via the `gh` CLI. See `docs/agents/issue-tracker.md`.

### Triage labels

Default vocabulary: `needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: one `CONTEXT.md` and `docs/adr/` at the repo root. See `docs/agents/domain.md`.

## Test policy

Tests exercise external behaviour only (spec #1, "Testing Decisions").

| Code | Required proofs | Coverage expectation |
| --- | --- | --- |
| Oracle, validity gate, trace-ID normalization, dedup, report writer, diff | seam 1: Go tests calling the CLI entry (`harness.Main(args)`) on hand-built run dirs under `testdata/`, asserting only on `report.json` / diff output | one asserted case per row of each decision table |
| Scenario loader | seam 1 | each rejection class |
| Stack composer, apps, recorder, fake intake, ledger collector, break injection | seam 2: `go test -tags e2e ./e2e` running `harness run` against the real compose stack | the scenario's asserted `report.json` values |
| Recorder header allowlist, ledger collector HTTP contract | own-layer Go test with `httptest`, in addition to seam 2 | each allowlist member kept, a credential header dropped; each status code |
| Compose, Dockerfile, YAML plumbing | none of its own | covered by seam 2 |

## Toolchain

Go and JDK 21 come from Homebrew: `export PATH=/opt/homebrew/opt/openjdk@21/bin:$PATH`. Docker is required for seam 2.

## tlc-implement

profile: standard
handoff: on
