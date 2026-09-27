# Akai Ito Application Test: Design

> **Status:** design settled through a grilling session on 2026-09-26. Nothing built yet.
> **Companion to:** [`akaiito/Design.md`](../akaiito/Design.md) (section numbers written as "AI §n" refer to it).
> **License:** Apache-2.0 · **Language:** Go (harness), Java and Go (test apps) · **Doc language:** English
> **Tags:** #project #testing #tracing #kafka #sqs #sns #datadog #opentelemetry

---

## 1. One-paragraph summary

`akaiito-application-test` is a **ground-truth test bench for Akai Ito**. It runs real producer and consumer apps with real tracers against real brokers (local Kafka and LocalStack SQS/SNS), and it breaks trace propagation on purpose using the same mechanisms that break it in production. Each app writes a **ledger** that records which producer sent every message, and that ledger is the **answer key**. The harness captures what the Datadog agent sends (Akai Ito's input) and what reaches the intake (Akai Ito's output), then scores Akai Ito's census and links against the answer key with a **tiered oracle**. Akai Ito is treated as a black box. Before Akai Ito exists, the bench runs in **baseline mode** with nothing in the path, and produces recorded **fixtures** plus the true orphan counts. Later it also covers the open facts in AI §11, fault injection against AI §5.4, a realistic payments sandbox, and a load/soak rig.

---

## 2. The problem

Akai Ito's value depends on numbers nobody can check by eye:

- The v0 exit criterion (AI §7) is a go/no-go decision based on census numbers. A census that miscounts orphans leads to the wrong call.
- Rung 2 (AI §6.2) gives a confidence of `1/N`. Showing that the true producer is among the N candidates takes an independent record of who actually sent what.
- AI §8 and §11 show that tracer behaviour (which tags are set, on which side, in which version) is where most of the design risk sits. Synthetic payloads would only encode what we already believe.
- The operational invariants in AI §5.4 (fail-open, bounded hold, idempotency, degraded mode, multi-replica correctness) are only meaningful if something actively violates their preconditions.

The bench exists so every one of these claims can be checked against a known answer.

### What this bench deliberately is not

- **Not Akai Ito's unit or contract test suite.** Join-core tests live in the akaiito repo and consume this repo's fixtures (§8).
- **Not a white-box test.** It never imports Akai Ito code. It sees only the wire.
- **Not a CI gate or a public demo.** It is a personal development tool. It should still run with one command.
- **Not a synthetic span generator.** Every span comes from a real tracer. Replay only resends bytes a real tracer produced.
- **Not a test of Akai Ito's internals.** Properties invisible on the wire, such as minimal retention (AI §5.4: the store holds only join keys, time windows and span refs), are owned by the akaiito repo, ideally enforced by the stored record type plus store unit tests.

---

## 3. Core insight: the app knows the truth the tracer lost

A producer app always knows the native ID of what it sent (Kafka `RecordMetadata`, SQS/SNS `SendMessage`/`Publish` response) and the trace/span ID of its active span, even when its tracer never tags either one. A consumer app always knows which message it received. If both sides write that down **outside the telemetry path**, and the message body carries a marker that survives header stripping, the join is exact regardless of what the tracer or the broker did. That record is the answer key Akai Ito is scored against.

---

## 4. Decisions

| # | Decision | Chosen | Rejected |
|---|---|---|---|
| T1 | Purpose | All four, in priority order: ground truth, fact verifier, realistic sandbox, load/soak | Any single purpose |
| T2 | Audience | The author only | CI gate, public demo |
| T3 | Coupling to Akai Ito | **Black box**: Akai Ito runs as a container in the path; only inputs and outputs are observed | Also importing the join core |
| T4 | Span source | **Real tracers**, captured into recorded fixtures, replayed for determinism and load | Real only, synthetic only |
| T5 | Environment | Local docker compose (Kafka, LocalStack, real Datadog agent, fake intake); **opt-in real Datadog** mode | Real Datadog only, fully offline only |
| T6 | Broker coverage | Kafka, SQS, SNS to SQS, Lambda batch; delivered in that order | Kafka only |
| T7 | Tracers | Java (dd-trace-java, OTel Java agent, chosen at launch) and Go (dd-trace-go, OTel Go) | Java only, adding Python/Node |
| T8 | Ground truth | **Ledger + body marker**: `run_id` + `seq` in the message body; producer and consumer ledger rows | Scenario-declared counts only, deriving truth from the backend |
| T9 | Scenario definition | One parameterized app per language, driven by **declarative YAML scenarios** | Code per scenario, Go test suite |
| T10 | Harness language | Go | Python, bash |
| T11 | Fixtures | Versioned in this repo with a manifest; **akaiito pulls tagged sets** | Copied into akaiito, local only |
| T12 | Baseline mode | Yes: the bench runs with no Akai Ito in the path | Waiting for Akai Ito v0 |
| T13 | Oracle | **Tiered**: exact where the design promises exactness, tolerance where it promises an estimate | Report only, all exact |
| T14 | Tracer versions | **Pinned matrix** per scenario, including a pre-PR #10843 dd-trace-java | Latest only |
| T15 | Operational invariants | A dedicated **fault scenario class** | Data-path scenarios only |
| T16 | Recording hygiene | Body bytes + allowlisted headers; API keys never touch disk; synthetic message data only | Full HTTP dumps |
| T17 | Sandbox | A small **payments flow** composed from the parameterized apps | Replay-driven loop, deferring the design |
| T18 | Load generation | **Fixture replay** at a target rate + **live apps** at high rate | Replay only, live only |
| T19 | Ledger transport | **HTTP to a ledger collector**; apps buffer and retry without blocking message flow; an incomplete ledger marks the run INVALID | JSONL on a shared volume, blocking on ledger acks |
| T20 | Break injection | **Real mechanisms** (bridges, sampling rules, attribute limits, delivery settings, tracer versions) | App flags that skip injection |
| T21 | Report | **HTML rendered from `report.json`**; the JSON is the source of truth and is diffable | Terminal table, Datadog dashboard, HTML without JSON |
| T22 | Akai Ito internals (minimal retention) | **Out of scope** for the bench; owned by akaiito's store tests | Inferring from store size, inspecting Valkey |
| T23 | LocalStack plan | **Free Hobby plan** with a personal auth token, pinned calendar version | LocalStack Pro, real AWS for L1 |

---

## 5. Architecture

```
 scenario.yaml ──► harness ──► docker compose (per run)

 producer app ──► broker ────────────► [bridge] ──► consumer app
 (tracer X)       Kafka | LocalStack                (tracer Y)
     │                                                  │
     ├── spans ──────────► Datadog agent ◄──── spans ───┤
     │                        │ DD_APM_DD_URL           │
     │                        ▼                         │
     │                   recorder (tee: captures input) │
     │                        │                         │
     │                        ▼                         │
     │               [ Akai Ito | nothing ]  ◄── baseline mode = nothing
     │                        │           └── metrics ──► fake metrics sink ──(opt-in)──► Datadog
     │                        ▼                         │
     │                   fake intake (captures output) ──(opt-in)──► Datadog intake
     │                                                  │
     └── ledger rows (HTTP) ──► ledger collector ◄──────┘

 harness: ledger + recorder captures + intake captures + metrics ──► oracle ──► report.json ──► report.html
```

### 5.1 Components

| Component | Role |
|---|---|
| **Harness** (Go CLI) | Reads a scenario, brings up the compose stack, drives the run, collects artifacts, runs the oracle, writes the report. Subcommands: `run`, `record`, `replay`, `report` (names are spec material) |
| **Parameterized apps** | One Java app and one Go app. Config picks role (producer, consumer, bridge), broker, destination, rate, message count, and break injection. The tracer is chosen at launch (`-javaagent` for Java, build tag or init for Go) |
| **Bridge** | A consumer-then-producer relay that re-produces without forwarding headers, like Kafka Connect or a custom bridge (AI §2). It writes ledger rows on both sides: `bridge_in` for the consumed `seq`, then `bridge_out` with a **new** `seq` in the outgoing body marker and `parent_seq` set to the consumed one. A consumer's true producer is always the last writer of its marker (the bridge, not the origin producer) |
| **Datadog agent** | Real agent, pinned version. `DD_APM_DD_URL` points at the recorder; it redirects only the APM writer traffic (`/api/v0.2/traces`, `/api/v0.2/stats`). OTLP ingest is off by default and is enabled with `DD_OTLP_CONFIG_RECEIVER_PROTOCOLS_GRPC_ENDPOINT` / `DD_OTLP_CONFIG_RECEIVER_PROTOCOLS_HTTP_ENDPOINT`; OTel-traced apps reach the agent through it. The agent requires `DD_API_KEY` even against the fake intake: a dummy value suffices locally (it retries while the intake is unreachable); real Datadog mode needs the real key, passed through env only |
| **LocalStack** | SQS, SNS and Lambda emulation, pinned to a calendar-version image tag, not `latest`. Starts only with `LOCALSTACK_AUTH_TOKEN` (free Hobby plan), read from the developer's env or a gitignored `.env`; never logged, committed or captured in fixtures |
| **Recorder** | Pass-through tee in front of Akai Ito. Captures every agent request (body bytes + allowlisted headers) byte for byte, then forwards it |
| **Akai Ito** | The system under test, as a pinned container image or a local build. Absent in baseline mode |
| **Fake intake** | Accepts what Akai Ito (or the recorder, in baseline mode) forwards, stores it, and returns success. It can be told to fail (§6.5). In real Datadog mode it also tees to the real intake |
| **Fake metrics sink** | Accepts Akai Ito's Datadog metrics submissions for the oracle. Tees to real Datadog in opt-in mode |
| **Ledger collector** | HTTP endpoint that receives ledger rows and writes them to `runs/<run_id>/ledger/` |

### 5.2 The ledger

Each app writes a row to the collector for every message it produces, consumes or bridges:

```
LedgerRow {
  run_id, scenario_id, seq         // seq is unique per produced message within a run
  side:           producer | consumer | bridge_in | bridge_out
  parent_seq                        // bridge_out only: the seq this row re-produces
  service, language, tracer, tracer_version
  broker:         kafka | sqs | sns
  destination:    topic | queue URL | topic ARN
  native_id:      (partition, offset) | message_id   // what the client returned or delivered, not what the tracer tagged
  trace_id, span_id                 // producer: active span at send; consumer: the span context the tracer associates with this record
  ts_ns                             // wall clock at send/receive
}
```

- **Body marker:** every message body is `{"akt": {"run": <run_id>, "seq": <seq>}, "data": ...}`. Consumers find it after unwrapping any SNS envelope. The marker never goes in headers or message attributes, because scenarios strip or exhaust those on purpose.
- **Consumer span ref:** for auto-instrumented consumers the consumer span is not reliably the active span at record-handling time (the OTel Java agent's receive span ends when `poll()` returns; process spans depend on flags). The consumer row records the per-record span context the tracer associates with the record, per tracer (§10).
- **Batches:** a batched receive (S4) or a Lambda invocation (L1, L2) writes one consumer row per `seq`; the rows share `trace_id`/`span_id` and each carries its own `native_id`.
- **Trace ID normalization:** the Datadog wire `trace_id` is 64-bit, with the high 64 bits in `meta["_dd.p.tid"]`; tracer APIs return 128-bit IDs. Ledger trace IDs are compared with captured ones on the full 128 bits reconstructed from `trace_id` + `_dd.p.tid`, falling back to the low 64 bits when `_dd.p.tid` is absent. The same rule applies to spans after agent OTLP ingest.
- **Delivery:** apps send rows to the collector asynchronously with a bounded buffer and retries. Ledger delivery never blocks or delays message flow, since that would shift the timing rung 2 depends on.
- **Validity gate:** after a run the harness checks that every `seq` has a producer row and the expected consumer rows (fan-out aware). A run with missing rows is reported **INVALID**, never as an Akai Ito failure. Consumer rows are at-least-once (SQS redelivery, Kafka rebalances): the oracle dedupes on `(seq, consumer service, span_id)`, extra consumer rows never make a run INVALID, and each redelivery is a distinct consumer span for O1.
- **The ledger lives outside the telemetry path.** It is never sent to the agent, the recorder or Akai Ito.

### 5.3 Modes

| Mode | Path | Produces |
|---|---|---|
| **Baseline** | agent → recorder → fake intake | Fixtures, ground-truth census (true orphans per tier, repairable fraction per rung) |
| **Akai Ito** | agent → recorder → Akai Ito → fake intake | Everything baseline produces, plus the oracle score for Akai Ito |
| **Real Datadog** (opt-in) | Either of the above; fake intake and metrics sink tee to real Datadog | Backend-side checks: link rendering, dangling-parent spot checks (AI §7) |
| **Replay** | harness replayer → Akai Ito → fake intake | Deterministic reruns and load (§6.7) |

---

## 6. Scenarios

A scenario is a YAML file. It names the topology (apps, tracers, versions, broker settings), the break injection, the traffic shape, and the expected outcome per oracle tier. Illustrative shape (the exact schema is spec material):

```yaml
id: K2-otel-producer              # OTel producer variant (rung 1); the dd-java variant expects rung: time_window
broker: kafka
topology:
  - {role: producer, lang: java, tracer: otel-java, version: latest, topic: orders}
  - {role: bridge,   lang: java, tracer: otel-java, version: latest, from: orders, to: orders-copy, forward_headers: false}
  - {role: consumer, lang: java, tracer: otel-java, version: latest, topic: orders-copy}
traffic: {messages: 500, rate_per_s: 20}
expect:
  certain_orphans: from_truth        # computed from ledger + captures, compared exactly
  links: {rung: native_id, precision: 1.0}
```

### 6.1 Kafka set (milestone 1)

| ID | Setup | Tests |
|---|---|---|
| K0 | Healthy propagation, dd-java both sides | Control: zero orphans, zero links |
| K1 | Healthy propagation, OTel Java both sides | Control for the OTel path through agent OTLP ingest |
| K2 | Bridge re-produces without headers | Certain orphans. The consumer's true producer is the bridge's producing side: rung 1 when it is OTel Java, rung 2 when it is dd-java |
| K3 | Tracer-side sampling rule (`DD_TRACE_SAMPLING_RULES`) drops the producer trace | Dangling-parent orphans (estimate tier) |
| K4 | dd-java producer (no offset), rate sweep on one topic | Rung-2 ambiguity: N and confidence versus message rate |
| K5 | Topic with `message.timestamp.type=LogAppendTime` | The rung-2 timestamp derivation fails as AI §6.2 predicts; the census counts it |
| K6 | Clock skew between producer and consumer containers (libfaketime, §10) | Skew exposure in link attributes and the census |
| K7 | Mixed tracers (OTel producer, dd consumer; dd-go and OTel Go variants) | Cross-tracer extraction |

### 6.2 SQS set (milestone 2)

| ID | Setup | Tests |
|---|---|---|
| S0 | Healthy propagation | Control |
| S1 | Producer fills all 10 message attributes | No slot left for the tracer attribute: orphans. Before scoring, the run asserts the tracer attribute was rejected or absent on the received message, else INVALID (LocalStack may silently accept an 11th attribute; real AWS caveat, §10) |
| S2 | dd-trace-java from before PR #10843 | No APM context on `SendMessage`: orphans |
| S3 | Datadog cloud payload tagging on | Deterministic mode (rung 1 on `MessageId`) |
| S4 | Batch receive (`MaxNumberOfMessages` > 1) | Multi-message receive spans; the `native_ids` cap |

### 6.3 SNS to SQS set (milestone 3)

| ID | Setup | Tests |
|---|---|---|
| N0 | Raw message delivery on | Envelope absent; SNS `MessageId` unavailable on the SQS side |
| N1 | Raw message delivery off | Context wrapped in the envelope; SNS `MessageId` present in the envelope |
| N2 | Subscription map supplied to Akai Ito | Rung 2 across topic ARN and queue, if Akai Ito implements the map (AI §6.2) |

### 6.4 Lambda batch set (milestone 4)

| ID | Setup | Tests |
|---|---|---|
| L1 | SQS event source mapping, batch > 1, on the LocalStack free Hobby plan | One invocation, N parent messages |
| L2 | Kafka event source mapping | Same for Kafka; plan coverage and feasibility open (§10) |

### 6.5 Fault class

Runs against Akai Ito only. It checks AI §5.4 by breaking its preconditions:

| ID | Injection | Expected |
|---|---|---|
| F1 | Valkey stopped mid-run | Degraded mode: no hold, immediate forward, degraded health + metric |
| F2 | Malformed `AgentPayload` | Forwarded unmodified (fail-open) |
| F3 | Unknown path or unrecognized payload version | Raw bytes forwarded, no repair |
| F4 | Fake intake returns 5xx or times out | No span loss attributable to Akai Ito; agent retry behaviour preserved |
| F5 | The same payload sent twice | No double-counted census, no double-attached links |
| F6 | Load over the in-flight hold cap | Immediate forward + hold-overflow signal |
| F7 | Two Akai Ito replicas behind a load balancer, shared Valkey | Correct links; certain-orphan count stays exact; dangling-parent over-count measured |
| F8 | Producer spans delayed past the hold window | Consumer forwarded unlinked; late match counted |
| F9 | Non-trace endpoints. Live agent: stats (the only non-trace endpoint it sends under `DD_APM_DD_URL`). Info, telemetry, remote config and the rest of the AI §5.2 list: replayed or crafted requests (a telemetry URL override on the agent is an option to verify, §10) | Byte-for-byte equality between recorder capture and intake capture |
| F10 | High-cardinality run (many topics/queues and services) | Metrics stay capped at topic x service x broker (AI D20) |
| F11 | Orphan consumer chunks carrying drop priority, via replay with the priority rewritten (the live agent drops most such chunks before forwarding) | Links still added; sampling tags byte-identical; drop-priority repairs counted |
| F12 | Pass-through API key on agent requests; distinct metrics key configured | The pass-through key never appears in captured Akai Ito logs; the metrics sink sees only the dedicated key in its auth header. Store contents are out of scope (§2) |

### 6.6 Realistic sandbox

A small payments flow built from the same parameterized apps: `api → orders (Kafka) → ledger-service → settlements (SQS) → notifier (SNS → SQS)`. It runs continuously at low rate with a configured mix of break points (a bridge on one hop, a sampling rule, one old tracer). It is meant to be viewed in real Datadog mode. The ledger and oracle still run, so the sandbox also produces a score.

### 6.7 Load and soak

- **Replay load:** the harness replayer resends recorded payloads to Akai Ito at N requests/s, re-stamping trace IDs, span IDs and timestamps so joins stay unique and inside the time windows. It measures throughput, added latency, memory and store size against the AI §5.5 envelope.
- **Replay surgery:** the replayer decodes each `AgentPayload` (handling `Content-Encoding` and the indexed `idx` tracer-payload variant), re-IDs every span consistently (`trace_id`, `span_id`, `parent_id`, `_dd.p.tid`, IDs inside `_dd.span_links`), shifts `start` while keeping `duration`, and re-encodes (datadog-agent `pkg/trace/pb`). Timestamp-bearing tags must stay consistent with the shifted span times: rung 2 derives produce time from the consumer's `record_queue_time_ms`, so producer and consumer spans of one join are shifted by the same offset and `record_queue_time_ms` is kept valid against them.
- **Live load:** apps at high rate on a single topic, to measure rung-2 ambiguity on busy topics (AI §6.2), which replay can't produce.
- **Soak:** long runs that watch memory bounds, TTL expiry and unmatched-producer metrics.

---

## 7. The oracle

The harness computes the truth from the ledger plus the recorder captures, then scores Akai Ito's metrics and intake captures against it. Every trace ID comparison uses the 128-bit normalization rule (§5.2); consumer rows are deduped as in the validity gate.

| Tier | Truth | Akai Ito output | Pass rule |
|---|---|---|---|
| **O0 Validity** | Ledger completeness | n/a | Incomplete ledger → run INVALID |
| **O1 Certain orphans** | Consumer spans that are roots in their chunk, joined to their true producer through the ledger | Census metric | **Exact** match per topic/queue x service x broker, split into repairable and not |
| **O1v Volume and cause** | Producer and consumer span volume per topic/queue x service x broker from ledger + captures; orphan rate per break-cause signature, with the cause known from the scenario's injection | Census volume and per-signature metrics (AI §7) | Volume **exact**; each orphan attributed to the signature of its injected cause |
| **O2 Dangling-parent orphans** | Non-root consumer spans whose `parent_id` is absent from recorder captures received within a truth window that mirrors Akai Ito's configured seen-span-ID window exactly (AI §7 tier (b); length taken from the scenario / Akai Ito config): anchored at the moment the consumer span's request reaches the recorder, a lookback of the configured length plus any look-ahead Akai Ito's spec defines (none by default), each classified through the ledger by trace ID: **lost parent** when the consumer's trace ID matches its true producer's ledger trace ID (a real span of the producer trace that did not reach the recorder in the window: tracer sampling, drop priority, agent drop, lateness); **unknown parent** when it matches no ledger trace. The ledger holds only producer/consumer/bridge span refs, so the missing parent may be any span of that trace. Reported only, not scored: the full-run split into **late parents** (captured after that window closes but within the run) and **never-captured parents** | Census metric | Akai Ito's estimate within the scenario's declared tolerance of the union of lost and unknown parents (the design calls this tier an estimate); the class split and the full-run split are reported |
| **O3a Rung-1 links** | Ledger producer span for each consumer | `_dd.span_links` in the intake capture | Precision **100%**; recall equals the truth-repairable fraction; `akaiito.confidence = 1.0` |
| **O3b Rung-2 links** | Ledger producer span + true candidate set: captured producer spans on the same destination whose window contains the derived timestamp and that reached the recorder before the consumer's hold expired and within TTL (late arrivals, F8, excluded) | `_dd.span_links` | N <= k: the true producer is among the links. N > k: k links, each pointing at a member of the true candidate set. Always `akaiito.candidates = N`; confidence `1/N` with the single-candidate ceiling |
| **O4 Invariants** | Fault injected (§6.5) | Intake capture, metrics, health | Per the fault table; byte-for-byte comparisons are exact |
| **O5 Unmatched producers** | Captured producer observations with no matchable consumer observation (captured, carrying a join key the ledger says matches) inside TTL. Reported separately, not scored: **dark consumers**, ledger producers with no consumer rows at all | Unmatched-producer metric | Exact after TTL |

In baseline mode only O0 is scored; O1 to O5 truths are computed and written to the report and the fixture manifest's expected values.

### Report

Each run writes `runs/<run_id>/report.json` (scenario, stack versions, truth, observed, per-tier verdicts), which is the source of truth. `report.html` is rendered from it. The report can diff two runs, typically baseline versus Akai Ito on the same fixtures, or two Akai Ito versions.

---

## 8. Fixtures

- A fixture set is a directory per scenario run: recorder captures (body bytes + allowlisted headers), the ledger, and a manifest (scenario ID, tracer + version, agent version, run ID, truth summary).
- Sets are committed to this repo and published under git tags (for example `fixtures-v1`). Akai Ito's join-core contract tests (AI §5.1) pull a tagged set.
- **Hygiene:** the recorder keeps only `Content-Type`, `Content-Encoding` and agent/tracer version headers. `DD-Api-Key` and any other credentials never reach disk. Apps only emit synthetic data, so captures are safe to commit.
- Recording is a side effect of any baseline run; `record` is a convenience that runs a scenario in baseline mode and promotes its captures into a fixture set.

---

## 9. Delivery plan

The order follows T1 (truth, facts, sandbox, load) and T6 (Kafka, SQS, SNS, Lambda), and tracks Akai Ito's own stages.

| Stage | Ships | Needs Akai Ito |
|---|---|---|
| **M0: Skeleton** | Compose stack, recorder, fake intake, fake metrics sink, ledger collector, harness `run` + `report`, Java app with dd-java and OTel Java, K0 to K3 in baseline mode | No |
| **M1: Kafka** | K4 to K7, Go app, first fixture tag | No |
| **M2: Facts** | Real Datadog mode; scenarios that settle AI §11 facts (§10) | No, except link rendering (§10) |
| **M3: SQS, SNS** | S0 to S4, N0 to N2, fixture tag | No |
| **M4: Akai Ito v0** | Akai Ito mode, oracle tiers O1, O1v, O2, O4 (read-only faults F2 to F4, F9, F10, F12) | v0 |
| **M5: Lambda** | L1 (L2 if feasible) | No |
| **M6: Sandbox** | Payments flow | Optional |
| **M7: Akai Ito v1 + load** | Oracle O3, O5; faults F1, F5 to F8, F11; replay and live load; soak | v1 |

**Exit criterion for M0:** a baseline K2 run whose report shows the exact number of certain orphans produced by the bridge, with a complete ledger, and a committed fixture set Akai Ito can read.

---

## 10. Open items

### Facts the bench should verify (feeds AI §11)

- The tag Datadog cloud payload tagging produces for SQS/SNS `MessageId`, and whether receive spans carry it (S3, S4).
- Whether any tracer records the SNS `MessageId` from the delivery envelope on the SQS side (N1).
- Whether the OTel Java AWS SDK instrumentation sets `messaging.message.id` on send and receive.
- Whether proxy-added `meta["_dd.span_links"]` render in the Datadog UI like tracer-generated ones. Before Akai Ito v1 exists this needs something to add a link; a test-only linker in the harness is one option, not yet decided.

### Facts about the bench itself

- Whether the Datadog agent's OTLP ingest keeps `messaging.kafka.offset` / `messaging.message.id` as span tags Akai Ito can read (affects K1, K2, K7).
- Which Kafka integration dd-trace-go and OTel Go instrument, and which tags each records.
- LocalStack fidelity: SNS to SQS envelope shape, `MessageId` behaviour, the 10-attribute limit (S1 asserts it before scoring; real AWS is the authority). A real AWS opt-in mode may be needed for N0 to N2 if LocalStack diverges.
- Whether a Kafka event source mapping (L2) is available on the LocalStack Hobby plan, and whether it can point at the compose Kafka.
- Which span each tracer treats as active during record handling, and which span Akai Ito must link (the per-record consumer context the ledger records, §5.2).
- Whether the agent's telemetry URL can be overridden to the recorder, so F9 covers telemetry with the live agent instead of crafted requests.
- Go clock skew for K6: Go reads time through the vDSO rather than libc, so libfaketime likely does not affect Go apps; a Go-side method is needed.

### Resolved facts about the bench

- `DD_APM_DD_URL` redirects only the agent's APM writer traffic (`/api/v0.2/traces`, `/api/v0.2/stats`), not tracer telemetry, remote config or info (datadog-agent `pkg/config/setup/apm.go`, `pkg/trace/writer/stats.go`). F9 is scoped accordingly.
- Agent OTLP ingest is off by default and enabled through `DD_OTLP_CONFIG_RECEIVER_PROTOCOLS_*_ENDPOINT`; the agent needs `DD_API_KEY` set, and a dummy value works against a fake intake (to confirm during M0).
- Head-sampled producers never reach the recorder: tracer-side rules (`DD_TRACE_SAMPLING_RULES`) drop at the tracer, agent-side priority sampling drops at the agent before forwarding (datadog-agent `pkg/trace/README.md`). K3 uses a tracer-side rule.
- The Datadog Lambda extension supports `DD_APM_DD_URL`, so L1 and L2 keep the recorder as capture point (docs.datadoghq.com/serverless/guide/agent_configuration).
- Since release 2026.3.0 (March 23, 2026) LocalStack ships as a single image that requires an auth token (`LOCALSTACK_AUTH_TOKEN`) to start. The free Hobby plan (non-commercial) matches the former Community image, which already supported Lambda with SQS event source mappings (localstack/localstack PR #11625 and its SQS ESM tests). L1 runs on the Hobby plan (blog.localstack.cloud/localstack-single-image-next-steps, localstack.cloud/pricing).
- Clock skew for K6 is injected with libfaketime via `LD_PRELOAD` in the consumer container (Java apps; Go is still open, above).

### Spec material

- Scenario YAML schema; harness CLI names; ledger collector API.
- Pinned tracer and agent versions per matrix entry, including the exact pre-PR #10843 dd-trace-java version and a build with the upstream producer-offset PR (AI §6.3).
- O2 tolerance and seen-span-ID window per scenario; the window's direction (lookback only, or lookback plus look-ahead) is taken from Akai Ito's v0 spec and mirrored in the O2 truth. The rung-2 candidate window used by O3b.
- Load targets, which follow Akai Ito's AI §5.5 figures once set.

---

## 11. Glossary

| Term | Meaning here |
|---|---|
| **Answer key** | The truth computed from the ledger and captures, which Akai Ito is scored against |
| **Ledger** | Per-message rows written by apps outside the telemetry path: who sent or received which message, with native ID and span ref |
| **Body marker** | `run_id` + `seq` in the message body; survives header stripping |
| **Baseline mode** | A run with no Akai Ito in the path; produces fixtures and true orphan counts |
| **Fixture set** | Recorder captures + ledger + manifest for one scenario run, committed and tagged |
| **Recorder** | The tee in front of Akai Ito that captures its exact input |
| **Fake intake** | The sink that captures Akai Ito's output (or tees it to real Datadog) |
| **Oracle tier** | One class of check with its own pass rule: exact, tolerance, or invariant |
| **INVALID run** | A run whose ledger is incomplete; not evidence about Akai Ito either way |
| **Bridge** | A relay app that re-produces messages without forwarding headers, reproducing the intermediary break point |
