# M0 + M1: baseline bench (Kafka, Java and Go, fixtures-v1)

Sources:

- https://github.com/gutrovalim/akaiito-application-test/issues/1 - spec: modules, contracts (LedgerRow, body marker, seq, 128-bit rule, dedup key, report.json, manifest), testing seams
- issues #2 to #13 - one slice each, acceptance criteria below carry them verbatim or sharpened
- `Design.md` - decisions T1 to T23, ledger (§5.2), oracle tiers (§7), fixtures (§8), open facts (§10)
- `~/repos/akaiito/Design.md` §5.1, §6, §7 - what "repairable", rung 1 and rung 2 mean (the truth mirrors them)

## Out of scope

- #14 real Datadog mode, #21 facts - no real `DD_API_KEY` in env; the last criterion of #14 cannot be proven
- #15 to #20, #26 to #28 - need `LOCALSTACK_AUTH_TOKEN`, not in env
- #22 to #25, #29 to #36 - need an Akai Ito container; `~/repos/akaiito` has no code
- Scoring O1 to O5 - baseline scores only O0 (Design §7); O1, O1v, O2 and rung-2 truths are computed and reported with verdict `REPORTED`
- O3a/O3b/O5 truth beyond the rung-2 candidate set of #10
- CI; `git push` of commits or the `fixtures-v1` tag (needs an explicit go-ahead)

## Landing

One Go module at the root (`github.com/gutrovalim/akaiito-application-test`): `cmd/harness` (CLI), `cmd/services` (recorder, fake intake, ledger collector, fake metrics sink in one binary and one image), `internal/...`. Payload decoding reuses `github.com/DataDog/datadog-agent/pkg/proto` `pbgo/trace` and `pbgo/trace/idx` (v0.83.2) rather than a hand-written decoder. Java app under `apps/java` (Maven, Java 21), Go app under `apps/go` (its own module, so tracer deps stay out of the harness).

| One-way door | Literal shape | Alternative rejected |
| --- | --- | --- |
| CLI | `harness run <scenario.yaml> [--runs-dir runs]`, `harness report <run_dir>` (writes `report.json` then `report.html`), `harness diff <a/report.json> <b/report.json>` (JSON to stdout), `harness record <scenario.yaml> [--fixtures-dir fixtures]` | `report --diff` flag - diff has two inputs and no run dir |
| Run directory | `runs/<run_id>/{scenario.yaml, stack.json, ledger/<service>.jsonl, recorder/<NNNNNN>.{meta.json,body}, intake/<NNNNNN>.{meta.json,body}, report.json, report.html}`; `run_id` = `<scenario_id>-<UTC yyyymmddThhmmssZ>-<4 hex>` | one JSONL for all captures - body bytes must stay byte-exact, base64 in JSONL doubles fixture size |
| Capture meta | `{"seq":n,"ts_ns":…,"method":"POST","path":"/api/v0.2/traces","headers":{…allowlisted only}}`; allowlist: `Content-Type`, `Content-Encoding`, `Datadog-Meta-Lang`, `Datadog-Meta-Lang-Version`, `Datadog-Meta-Tracer-Version`, `User-Agent`, `X-Datadog-Reported-Languages`, `Dd-Agent-Version` (case-insensitive) | denylist of credential headers - a new credential header would leak |
| Ledger collector API | `POST /v1/rows`, body = JSON array of LedgerRow, `204` on success, `400` on undecodable body; appended to `ledger/<service>.jsonl` | one row per request - bursts at high rate cost a request per message |
| LedgerRow JSON | snake_case per spec; `trace_id` 32 lowercase hex, `span_id` 16 lowercase hex; `native_id` = `{"partition":p,"offset":o}` or `{"message_id":"…"}`; `parent_seq` omitted unless `bridge_out`; `ts_ns` int64 | decimal Datadog IDs - OTel API and 128-bit IDs are hex-native; decimal hides the high 64 bits |
| Body marker | `{"akt":{"run":"<run_id>","seq":<int>},"data":{…synthetic}}`; `seq` starts at 1 per producing app and is globally unique by prefixing: producer entry `i` uses `seq = i*1_000_000 + n` | a shared seq service - another moving part on the message path |
| Scenario YAML | `id`, `broker`, `topology[]` (`name`, `role`, `lang`, `tracer`, `version`, `topic` or `from`/`to`, `forward_headers`, `sampling`, `clock_offset`, `env`), `topics{<name>: {partitions, config{}}}`, `traffic` (`messages`, `rate_per_s`) or `traffic.phases[]`, `expect` (`O0`, `O1.certain_orphans: from_truth|<int>`, `O2{tolerance, window_ms, lookahead_ms}`, `rung2{candidate_window_ms}`); unknown keys rejected | loose map - "invalid scenario rejected with a clear error" needs a closed schema |
| report.json | `{"schema":1,"run_id","scenario_id","mode":"baseline","stack":{…},"verdict":"PASS|FAIL|INVALID","tiers":{"O0":{"verdict","missing":[…],"deduped":n},"O1":{"verdict":"REPORTED","truth":{…}},"O1v":…,"O2":…,"rung2":…},"facts":{…}}`; verdict tokens `PASS`, `FAIL`, `INVALID`, `REPORTED` | per-tier files - the diff and the HTML want one document |
| Fixture set | `fixtures/<scenario_id>/{manifest.json, scenario.yaml, recorder/…, ledger/…}`; manifest `{"schema":1,"scenario_id","tracers":[{service,tracer,version}],"agent_version","run_id","o0","truth":{O1,O1v,O2,rung2}}` | copying `report.json` as the manifest - it carries paths and verdicts the contract tests must not depend on |
| Producer span ref | producer app wraps each send in its own span `akt.send` (via the tracer's API); the ledger records that span; the "true producer span" in captures is the messaging-producer child of it in the same trace | recording the tracer's produce span - its ID is not reachable from app code for dd-java |
| Consumer span ref | the span current inside the per-record iteration (`Span.current()` / tracer equivalent) | span current at `poll()` return - OTel receive span has ended by then (Design §5.2) |
| Repairable truth | an O1 orphan is `rung1` when its captured span and its true producer span both carry the ledger `(partition, offset)`; else `rung2` when the consumer carries `record_queue_time_ms` and the true producer span's `[start, end]` contains `consumer.start - record_queue_time_ms` widened by `rung2.candidate_window_ms`; else `not_repairable` | "consumer has any join key" - counts keys that match nothing |
| Headerless bridge | `forward_headers: false` = new `ProducerRecord` with no headers **and** the tracer's own Kafka propagation disabled on the bridge (`DD_KAFKA_CLIENT_PROPAGATION_ENABLED=false`, `OTEL_INSTRUMENTATION_KAFKA_PRODUCER_PROPAGATION_ENABLED=false`) | not copying headers only - the tracer re-injects its own, so nothing breaks |
| Default sampling | every app runs with 100% keep (`DD_TRACE_SAMPLE_RATE=1.0` / `OTEL_TRACES_SAMPLER=always_on`) unless the scenario sets `sampling` | agent default rates - silently drop healthy traces and make controls noisy |
| Pinned versions | agent `datadog/agent:7.83.2`, Kafka `apache/kafka:4.3.1`, dd-java `1.66.0`, OTel Java agent `2.31.1`, `kafka-clients 4.3.1`; every new dependency at least 7 days old | `latest` - T14 forbids it |
| dd-java Kafka producer native id | dd-trace-java tags the Kafka producer span with `partition` and `offset` from `RecordMetadata` as of **v1.62.0** (`KafkaProducerCallback.onCompletion`, upstream PR #11107, merged 2026-04-16), and the advice wraps the callback unconditionally, so an app-side callback cannot change it; `KafkaDecorator.onProduce` still sets only the topic and an explicitly set partition, which is what the design's verified fact was read from. K2's dd-java variant therefore runs a pinned **v1.61.1** (no producer native id, rung 2 only) and a pinned **v1.62.0** (rung 1) pair | a "no callback" variant - the advice replaces the callback, so it would behave identically and prove nothing |
| Go app tracers | Kafka via `IBM/sarama`; dd-trace-go v2 `contrib/IBM/sarama`, OTel via `github.com/dnwe/otelsarama`; tracer picked by `AKT_TRACER` env at init | confluent-kafka-go - cgo and librdkafka on arm64 images |
| Stack file and run_id | `stack.json` = `{"agent_version":"7.83.2","kafka_version":"4.3.1","tracers":[{"service","tracer","version"}]}`, copied verbatim into `report.json` `stack` (`{}` when absent); the report's `run_id` = basename of the run dir | `run_id` read from ledger rows - an empty ledger would leave the report without an id |
| Service name | a topology entry's service = `name` if set, else `akt-<role>`; auto names shared by several entries become `akt-<role>-<topology index>` (0-based); duplicate services rejected; ledger rows carry exactly this | always suffixing the index - every single-producer report would read `akt-producer-0` |
| Scenario field types | `version`, `clock_offset` strings; `sampling` a keep rate in [0,1] replacing the 1.0 default; `forward_headers` bool, default `true`; `env` and `topics.<t>.config` string maps; `traffic.phases[]` = `{messages, rate_per_s}`, exclusive with top-level `messages`/`rate_per_s`; `expect.O0` = `PASS\|INVALID`; `broker` required, one of `kafka\|sqs\|sns` | `sampling` as raw rule JSON - tracer-specific, OTel has no equivalent |
| O0 tier | `{"verdict","missing":[{"seq","side":"producer\|consumer\|bridge_in","service"?,"destination"?}],"deduped":n,"true_producers":{"<service>/<seq>":"<writer service>"}}`; `missing` sorted by seq, side, service; `deduped` counts consumer and `bridge_in` duplicates; `true_producers` covers every deduped consumer and `bridge_in` row | missing counts per service - the report must name the rows |
| Diff output | `{"tiers":{"<tier>":{"verdict":[a,b],"changes":[{"path","a","b"}]}}}`; `verdict` only when it differs; `path` = leaf path inside the tier (excluding `verdict`), dotted keys and `[i]` indices (`missing[0].seq`, `truth.groups[1].rung1`); absent side = `null`; tiers without differences omitted; exit 0 either way | JSON Patch - its ops say how to edit a, not what differed |
| Services binary and compose names | `services <recorder\|intake\|ledger\|metrics> [--addr :8080] [--dir /data] [--upstream URL]`; compose services `kafka` (`kafka:9092`), `kafka-init` (one-shot topic creation), `agent` (`agent:8126`), `recorder`, `intake`, `ledger`, `metrics` (all on `:8080`), one app service per topology entry named by its service; `runs/<run_id>/` bind-mounted at `/data`; recorder upstream `http://intake:8080`; images built locally as `akt-services:local` (`cmd/services/Dockerfile`, context repo root) and `akt-java:local` (`apps/java/Dockerfile`); compose project name = lowercased `run_id` (compose rejects upper case) | one port per service - every consumer of the stack must learn a port table |
| Agent env | `DD_API_KEY=aktdummyapikey000000000000000000` (Go const `stack.DummyAPIKey`), `DD_APM_DD_URL=http://recorder:8080`, `DD_DD_URL=http://metrics:8080` (all non-APM agent traffic and key validation hit the fake metrics sink), `DD_HOSTNAME=akt-agent`, APM on with non-local traffic, logs/process/remote-config/inventories off | default `DD_DD_URL` - the agent would call datadoghq.com with the dummy key |
| App config env | `AKT_ROLE` (`producer\|consumer\|bridge`), `AKT_RUN_ID`, `AKT_SCENARIO_ID`, `AKT_SERVICE`, `AKT_BROKER`, `AKT_BOOTSTRAP`, `AKT_TOPIC` (producer, consumer), `AKT_FROM`/`AKT_TO` (bridge), `AKT_PHASES` (`<messages>@<rate_per_s>` comma-separated; producer), `AKT_SEQ_BASE` (`i*1_000_000`), `AKT_EXPECT` (distinct seqs a reader waits for), `AKT_IDLE_TIMEOUT_S`, `AKT_LEDGER_URL`, `AKT_TRACER` (`dd-java\|otel-java`), `AKT_TRACER_VERSION`; the entrypoint launches `-javaagent:/opt/akt/agents/<AKT_TRACER>-<AKT_TRACER_VERSION>.jar`, both agent jars baked into the one image | CLI args - compose `environment` is already the per-entry override surface (`env`) |
| Span ID API (Java) | OpenTelemetry API (`GlobalOpenTelemetry.getTracer("akt")`, `Span.current().getSpanContext()`), with `DD_TRACE_OTEL_ENABLED=true` under dd-java; IDs are the API's hex (32/16) | dd-trace-api `CorrelationIdentifier` - decimal, low 64 bits unless 128-bit logging is on, and a second code path for OTel |
| LedgerRow `marker_in_headers` | `"marker_in_headers":true` on a consumer or `bridge_in` row when a received header key or value contains `akt`; omitted when false | a separate diagnostics file - the e2e reads one place |
| Done signal | an app signals done by exiting: producer after `flush()` and ledger drain; reader after `AKT_EXPECT` distinct seqs (exit 0) or `AKT_IDLE_TIMEOUT_S` without records (exit 3); the harness waits for every app container to exit, then until `recorder/` stops growing for 15 s (cap 120 s); it writes the report either way and exits 1 when an app exited non-zero or compose failed | harness polling ledger counts - couples the runner to the oracle |
| O0 expected seqs | each producer entry `i` is expected to write seqs `i*1_000_000 + 1 .. + N`, `N` = `traffic.messages` or the sum of `traffic.phases[].messages`; a missing one is `{"seq","side":"producer","service","destination"}`; bridges derive nothing | only seqs seen by readers - a producer writing no rows went unnoticed |
| Agent endpoint isolation | the "Agent env" row's `DD_DD_URL` catches the main forwarder only; EvP tracks (netpath, container lifecycle/image, sbom, synthetics, DSM, orchestrator explorer) use their own `*.datadoghq.com` URLs, so each is disabled explicitly (`DD_NETWORK_PATH_ENABLED`, `DD_CONTAINER_LIFECYCLE_ENABLED`, `DD_CONTAINER_IMAGE_ENABLED`, `DD_SBOM_*_ENABLED`, `DD_SYNTHETICS_COLLECTOR_ENABLED`, `DD_DATA_STREAMS_ENABLED`, `DD_ORCHESTRATOR_EXPLORER_ENABLED`, `DD_AGENT_TELEMETRY_ENABLED`), and the guarantee is the run's compose network, which is `internal: true` so no container can egress at all | leaving them on - each one posts to a real Datadog host with the dummy key, and the noise hides the APM traffic F9 must compare byte for byte |
| Residual agent traffic | the full agent image always starts the process agent and data plane (s6 "features detected from environment"), so `DD_PROCESS_CONFIG_PROCESS_DD_URL=http://metrics:8080` points them at our own sink instead of the internet; `failed to post` in `logs/agent.log` is the egress check (0 on a healthy run) | fighting the image's init - the process agent cannot be env-disabled, and its payloads reach only the fake sink, where the meta path and auth key stay available to F12 |
| Run dir contents | the run dir also holds `compose.yaml` (the generated stack), `metrics/` and `logs/` (per-service container logs plus `build.log`) | regenerating the compose file at report time - the artifact must show the stack that actually ran |
| Payload decoder | one package, `internal/trace`: `Decode(body, contentEncoding) ([]Span, error)` over `datadog-agent/pkg/proto` `pbgo/trace` + `pbgo/trace/idx` (v0.83.2), handling identity/gzip/zstd and both tracer-payload variants; `Span` exposes ids as 128-bit hex (`%016x` low half plus `Meta["_dd.p.tid"]` when present), `Meta`, `Metrics` and a `String`/`Int` accessor pair that reads both maps | reading only `Meta` - the Kafka join keys (`partition`, `offset`, `record.queue_time_ms`) live in the numeric `Metrics` map, so a Meta-only decoder sees no join key at all |
| Capture reader | one package, `internal/captures`, reusing `services.Meta` as the on-disk meta contract; `ReadTraces(dir)` returns every capture with its spans, and non-trace paths with none | a second meta struct in the reader - the writer and reader would drift |
| O1 truth shape | `{total, uncaptured, trace_id_mismatch, groups[]}`, each group `{destination, service, broker, total, rung1, rung2, not_repairable}` | counting orphans without the rung split - the design's census is reported per rung |
| O1v truth shape | `{groups[]}`, each `{destination, service, broker, producers, consumers}` | a single span count per service - producer and consumer volume are separate columns in the census |
| Trace-id mismatch | O1 truth carries `trace_id_mismatch`, the count of read rows whose ledger `trace_id` disagrees with the captured span under the 128-bit rule | comparing inside a test helper - the rule belongs where the join happens, and a mismatch is evidence of an app or tracer id bug |

- Nothing else in this change is hard to reverse

## Test policy (declared by #1 "Testing Decisions"; restated so each row gets a verdict)

| Code | Required proofs | Coverage expectation |
| --- | --- | --- |
| Oracle, validity gate, trace-ID normalization, dedup, report writer, diff (decide; reached through `harness report`/`harness diff`) | seam 1: Go tests that call the CLI entry (`harness.Main(args)`) on hand-built run dirs under `testdata/` and assert only on `report.json` / diff output | one asserted case per row of each decision table named in Coverage |
| Scenario loader (decides: accept/reject) | seam 1 | each rejection class in Coverage |
| Stack composer, apps, recorder, fake intake, ledger collector, break injection | seam 2: `go test -tags e2e ./e2e` running `harness run` against the real compose stack | the scenario's asserted `report.json` values |
| Recorder header allowlist, ledger collector HTTP contract (decide, cheap to isolate) | own-layer Go test with `httptest` in addition to seam 2 | each allowlist member kept, a credential header dropped; `204`, `400` |
| Compose/Dockerfile/YAML plumbing | none of its own | covered by seam 2 |

## Checks

### S1 - #2 report computes O0 validity · greenfield · ~12k

**C1** - `harness report` on a complete run dir writes `report.json` with `scenario_id`, `run_id` and `tiers.O0.verdict == "PASS"`
Proof: `go test ./cmd/harness -run '^TestReportO0CompletePass$'`

**C2** - A `seq` with no producer row sets `verdict == "INVALID"` and lists `{"seq":…, "side":"producer"}` in `tiers.O0.missing`
Proof: `go test ./cmd/harness -run '^TestReportO0MissingProducerInvalid$'`

**C3** - A missing expected consumer row (two consumers on one topic, one row absent) sets `INVALID` and names that consumer service and `seq` in `missing`
Proof: `go test ./cmd/harness -run '^TestReportO0FanOutMissingConsumerInvalid$'`

**C4** - Duplicate consumer rows with the same `(seq, service, span_id)` leave O0 `PASS` and set `tiers.O0.deduped` to the duplicate count; a redelivery (same seq and service, different `span_id`) also leaves `PASS`
Proof: `go test ./cmd/harness -run '^TestReportO0DedupAndRedelivery$'`

**C5** - A bridge chain (producer seq 1 -> `bridge_in` 1 -> `bridge_out` seq 2 with `parent_seq` 1 -> consumer seq 2) is `PASS`, and the report names the bridge service as the true producer of the consumer row (`tiers.O0.true_producers["<consumer>/2"] == "<bridge service>"`)
Proof: `go test ./cmd/harness -run '^TestReportO0BridgeChain$'`

**C6** - Invalid scenarios exit non-zero with an error naming the problem and write no `report.json`: unknown key, missing `id`, unknown `role`, consumer without `topic`, bridge without `from`/`to`
Proof: `go test ./cmd/harness -run '^TestReportRejectsInvalidScenario$'` (table-driven over the 5 cases)

### S2 - #9 report.html and diff · greenfield · ~8k

**C7** - `harness report` writes `report.html` rendered from `report.json` alone: deleting the ledger after `report.json` exists and re-rendering (`harness report --html-only <dir>`) yields the same HTML
Proof: `go test ./cmd/harness -run '^TestReportHTMLFromJSONOnly$'`

**C8** - `report.html` contains the scenario id, the agent version from `stack`, and each tier name with its verdict token
Proof: `go test ./cmd/harness -run '^TestReportHTMLShowsTiers$'`

**C9** - `harness diff a b` prints JSON whose `tiers.O0.verdict` is `["PASS","INVALID"]` when they differ and lists changed truth leaves as `{"path","a","b"}`; identical reports produce `{"tiers":{}}`
Proof: `go test ./cmd/harness -run '^TestDiffPerTier$'`

### S3 - #3 K0 end to end, dd-java · greenfield · ~40k

**C10** - The recorder stores only allowlisted headers (a `DD-Api-Key` header is not in the meta) and forwards body bytes and path unchanged to the upstream
Proof: `go test ./internal/services -run '^TestRecorderAllowlistAndForward$'`

**C11** - The ledger collector returns `204` for a valid row array and appends to `ledger/<service>.jsonl`; returns `400` for an undecodable body
Proof: `go test ./internal/services -run '^TestLedgerCollectorContract$'`

**C12** - The fake intake stores each request and returns `200`
Proof: `go test ./internal/services -run '^TestFakeIntakeStores$'`

**C13** - `harness run scenarios/k0.yaml` creates `runs/<run_id>/` with the `run_id` shape above, and two runs get different ids
Proof: `go test -tags e2e ./e2e -run '^TestK0$' -timeout 30m` (asserts dir + id regex; second id checked against the first run's)

**C14** - The K0 report has `tiers.O0.verdict == "PASS"`, ≥1 file in `recorder/` and ≥1 in `intake/`
Proof: `go test -tags e2e ./e2e -run '^TestK0$' -timeout 30m`

**C15** - Every consumer ledger row's `seq` matches the body marker the producer wrote, and no Kafka header on received records carries `akt` (checked by the consumer, which fails the row with `marker_in_headers:true` if present; the e2e asserts no row has it)
Proof: `go test -tags e2e ./e2e -run '^TestK0$' -timeout 30m`

**C16** - Producer rows carry `native_id.partition`/`offset` from `RecordMetadata` (every producer row has an offset, offsets unique per partition)
Proof: `go test -tags e2e ./e2e -run '^TestK0$' -timeout 30m`

**C17** - Ledger delivery does not block sending: the Java ledger client test, with the collector returning `503` for 5 s, sends 100 messages through the client in under 1 s and delivers all 100 rows after the collector recovers
Proof: `mvn -f apps/java -q test -Dtest=LedgerClientTest#nonBlockingWithRetries`

**C18** - Every image in the generated compose file carries an explicit tag that is not `latest`
Proof: `go test ./internal/stack -run '^TestComposePinsImages$'`

### S4 - #4 decode payloads, O1/O1v truth · greenfield · ~25k

**C19** - `AgentPayload` bodies decode for each `Content-Encoding` in {identity, gzip, zstd} and for both `tracerPayloads` and `idxTracerPayloads`
Proof: `go test ./cmd/harness -run '^TestReportDecodesEncodingsAndIdx$'` (table over 3 encodings x 2 variants)

**C20** - A ledger 128-bit trace ID matches a captured span whose low 64 bits are `trace_id` and high 64 are `_dd.p.tid`; with `_dd.p.tid` absent it matches on low 64 bits; a span with the same low 64 but a different `_dd.p.tid` does not match
Proof: `go test ./cmd/harness -run '^TestReportTraceID128Rule$'`

**C21** - Certain orphans (ledger consumer span that is root, `parent_id == 0`, in its captured chunk) are counted in `tiers.O1.truth.groups[]` keyed `destination x service x broker`, with `rung1`, `rung2`, `not_repairable` per the Repairable truth row
Proof: `go test ./cmd/harness -run '^TestReportO1CertainOrphansRepairableSplit$'` (one orphan per class, plus one non-orphan)

**C22** - `tiers.O1v.truth.groups[]` holds producer and consumer span volume per `destination x service x broker`; two redeliveries of one seq count as 2 consumer spans
Proof: `go test ./cmd/harness -run '^TestReportO1vVolumeCountsRedeliveries$'`

**C23** - Ledger rows whose span is absent from captures are counted in `tiers.O1.truth.uncaptured`, not as orphans
Proof: `go test ./cmd/harness -run '^TestReportO1Uncaptured$'`

**C24** - K0 baseline reports `tiers.O1.truth.total == 0` and `uncaptured == 0`
Proof: `go test -tags e2e ./e2e -run '^TestK0$' -timeout 30m`

### S5 - #5 K1 OTel Java via OTLP ingest · ~15k

**C25** - Tracer and version are applied at launch from the scenario (`dd-java@1.66.0` / `otel-java@2.31.1` agent jar passed as `-javaagent`), one app image for both
Proof: `go test ./internal/stack -run '^TestComposeTracerAtLaunch$'`

**C26** - The agent gets `DD_OTLP_CONFIG_RECEIVER_PROTOCOLS_GRPC_ENDPOINT`/`HTTP_ENDPOINT` only when some topology entry uses an OTel tracer
Proof: `go test ./internal/stack -run '^TestComposeOTLPOnlyWhenNeeded$'`

**C27** - K1 report: `O0 == PASS`, `O1.truth.total == 0`, `uncaptured == 0` (the consumer ref is the per-record span)
Proof: `go test -tags e2e ./e2e -run '^TestK1$' -timeout 30m`

**C28** - `facts.span_tags[]` has one entry per ledger service x side with `component` and the messaging tag keys seen (so the K1 report records whether `messaging.kafka.offset` survived OTLP ingest)
Proof: `go test ./cmd/harness -run '^TestReportFactsSpanTags$'`
Proof: `go test -tags e2e ./e2e -run '^TestK1$' -timeout 30m` (asserts a producer entry exists and its keys are recorded)

### S6 - #6 bridge and K2 · ~20k

**C29** - The bridge writes `bridge_in` for the consumed seq, then `bridge_out` with a new seq, `parent_seq` = consumed seq, and that new seq in the outgoing marker
Proof: `go test -tags e2e ./e2e -run '^TestK2DdJava$' -timeout 30m` (every `bridge_out` has a `bridge_in` with seq = its `parent_seq`; consumer seqs == bridge_out seqs)

**C30** - K2 dd-java pinned at v1.61.1: `O0 == PASS` and `O1.truth.total` equals the number of `bridge_out` rows, all in the `orders-copy x <consumer> x kafka` group, all `rung2`
Proof: `go test -tags e2e ./e2e -run '^TestK2DdJavaPreOffset$' -timeout 30m`

**C30b** - K2 dd-java pinned at v1.62.0: `O0 == PASS`, `O1.truth.total` equals the `bridge_out` count, all `rung1` (renegotiated with the user on 2026-09-27: #6 asked for "rung 2 only", which v1.62.0 falsifies)
Proof: `go test -tags e2e ./e2e -run '^TestK2DdJavaPostOffset$' -timeout 30m`

**C31** - K2 OTel: `O0 == PASS` and `O1.truth.total` equals the `bridge_out` count, all `rung1`
Proof: `go test -tags e2e ./e2e -run '^TestK2Otel$' -timeout 30m`

### S7 - #7 K3 and O2 truth · ~15k

**C32** - A non-root consumer span whose `parent_id` is absent from recorder captures received in `[anchor - window_ms, anchor + lookahead_ms]` (anchor = its request's `ts_ns`) is a dangling parent; a parent captured inside the window is not
Proof: `go test ./cmd/harness -run '^TestReportO2Window$'` (parent at window edge inside, 1 ms outside, look-ahead)

**C33** - A dangling parent is `lost_parent` when the consumer trace ID equals its true producer's ledger trace ID, else `unknown_parent`
Proof: `go test ./cmd/harness -run '^TestReportO2Classification$'`

**C34** - `tiers.O2.full_run` splits dangling parents into `late` (captured after the window, within the run) and `never_captured`, with verdict `REPORTED`
Proof: `go test ./cmd/harness -run '^TestReportO2FullRunSplit$'`

**C35** - The scenario loader accepts `expect.O2{tolerance, window_ms, lookahead_ms}` and rejects a negative `window_ms`
Proof: `go test ./cmd/harness -run '^TestReportRejectsInvalidScenario$'` (added case) and `^TestReportO2Window$`

**C36** - K3 baseline (tracer-side sampling on the producer) reports `O0 == PASS` and `O2.truth.lost_parent > 0`
Proof: `go test -tags e2e ./e2e -run '^TestK3$' -timeout 30m`

### S8 - #8 fixtures · ~10k

**C37** - `harness record scenarios/k2-dd-java.yaml` writes `fixtures/k2-dd-java/` with `recorder/`, `ledger/`, `scenario.yaml` and `manifest.json` holding every manifest field in Landing
Proof: `go test ./cmd/harness -run '^TestPromoteFixtureManifest$'` (promotion from a hand-built run dir)

**C38** - No file under `fixtures/` contains `DD-Api-Key` (case-insensitive), the dummy key value used by the stack, or the value of `LOCALSTACK_AUTH_TOKEN` when set
Proof: `go test ./fixtures -run '^TestFixtureHygiene$'`

**C39** - A committed `fixtures/k2-dd-java/manifest.json` has `o0 == "PASS"` and `truth.O1.total` equal to its ledger's `bridge_out` row count
Proof: `go test ./fixtures -run '^TestK2FixtureManifest$'`

### S9 - #10 K4 rate sweep, K5 LogAppendTime, rung-2 candidates · ~15k

**C40** - The rung-2 true candidate set of a consumer is the captured messaging-producer spans on the same topic whose `[start - w, end + w]` contains `consumer.start - record_queue_time_ms` (w = `rung2.candidate_window_ms`); reported as `tiers.rung2.truth.consumers[]{seq, n, true_producer_in_set}`
Proof: `go test ./cmd/harness -run '^TestReportRung2CandidateSet$'` (0, 1 and 3 candidates; w widens 1 case in)

**C41** - `tiers.rung2.truth.by_phase[]` gives `{rate_per_s, n_min, n_p50, n_max}` using the seq ranges of `traffic.phases`
Proof: `go test ./cmd/harness -run '^TestReportRung2ByPhase$'`

**C42** - K4 runs 3 rate phases (5, 50, 500 msg/s) and reports exactly one `by_phase` entry per phase, each with `n_min >= 0`
Proof: `go test -tags e2e ./e2e -run '^TestK4$' -timeout 45m`

**C43** - Topic configs from `topics.<name>.config` are applied at topic creation (compose creates the topic with them)
Proof: `go test ./internal/stack -run '^TestComposeTopicConfig$'`

**C44** - K5 (`message.timestamp.type=LogAppendTime`) reports `tiers.rung2.truth.derivation_failed` = count of consumers whose true producer is not in the candidate set; the e2e asserts O0 PASS and the field is present
Proof: `go test -tags e2e ./e2e -run '^TestK5$' -timeout 30m`

### S10 - #11 K6 clock skew · ~8k

**C45** - `clock_offset: "+5s"` on a Java entry sets `LD_PRELOAD` to libfaketime and `FAKETIME="+5s"` in that container only
Proof: `go test ./internal/stack -run '^TestComposeClockOffset$'`

**C46** - The report carries `tiers.rung2.truth.skew{declared_ms, consumers_outside_window}`; K6 e2e asserts O0 PASS and `declared_ms == 5000`
Proof: `go test -tags e2e ./e2e -run '^TestK6$' -timeout 30m`

**C47** - Design.md §10 records the Go-side skew finding (method tried, outcome) and moves it out of "open" if settled
Proof: `grep -n "Go clock skew" Design.md` shows the finding line, reviewed by the Verifier

### S11 - #12 Go app and K7 · ~30k

**C48** - The Go app supports producer, consumer and bridge and writes the same marker and LedgerRow JSON (a Go ledger client test round-trips a row through the collector and compares to the Java fixture row field set)
Proof: `go test ./... -run '^TestLedgerRowShape$'` in `apps/go`

**C49** - K7 variants `k7-otel-to-dd` (OTel Java producer, dd-java consumer), `k7-dd-go`, `k7-otel-go` each report `O0 == PASS`
Proof: `go test -tags e2e ./e2e -run '^TestK7$' -timeout 60m` (subtests per variant)

**C50** - The K7 Go reports' `facts.span_tags` record `component` and messaging tag keys for each Go tracer
Proof: `go test -tags e2e ./e2e -run '^TestK7$' -timeout 60m`

### S12 - #13 fixtures-v1 · ~5k

**C51** - `fixtures/` holds a set for each of k0, k1, k2-dd-java-pre, k2-dd-java-post, k2-otel, k3, k4, k5, k6, k7-otel-to-dd, k7-dd-go, k7-otel-go, each manifest with a `truth` object
Proof: `go test ./fixtures -run '^TestFixturesV1Complete$'`

**C52** - Hygiene holds over all sets
Proof: `go test ./fixtures -run '^TestFixtureHygiene$'`

**C53** - Local tag `fixtures-v1` points at the commit that adds the sets
Proof: `git rev-parse fixtures-v1^{commit}` equals `git log -1 --format=%H -- fixtures/`

## Swept

- validation: C6, C35 (scenario); C11 (ledger body)
- failure modes: C2, C3 (incomplete ledger -> INVALID, never an Akai Ito failure); C23 (uncaptured spans)
- idempotency and retry: C4 (dedup, redelivery); C17 (ledger retries)
- authorization: not in scope - local tool; key hygiene covered by C10, C38
- concurrency and ordering: C4 (at-least-once consumers); runs isolated per compose project name = run_id (C13)
- data lifecycle: `runs/` gitignored; fixtures committed only through `record` (C37)
- external-dependency failure: C17 (collector down); agent retries against intake are the agent's own behaviour
- state transitions: not in scope - no stateful entity
- observability: harness prints run_id, run dir and verdict on exit; apps log to container stdout, collected to `runs/<id>/logs/`

## Coverage

| Set (size) | Member -> proof | Unproven |
| --- | --- | --- |
| O0 outcomes (5) | complete C1 · missing producer C2 · missing fan-out consumer C3 · duplicate/redelivery C4 · bridge chain C5 | - |
| scenario rejections (6) | unknown key · missing id · unknown role · consumer no topic · bridge no from/to (all C6) · negative window C35 | - |
| Content-Encoding x variant (6) | identity/gzip/zstd x tracerPayloads/idx, table in C19 | - |
| 128-bit rule (3) | tid match · tid absent · tid mismatch (C20) | - |
| repairable classes (3) | rung1 · rung2 · not_repairable (C21) | - |
| O2 window edges (3) | inside edge · 1 ms outside · look-ahead (C32) | - |
| O2 classes (2) | lost C33 · unknown C33 | - |
| rung-2 candidate counts (3) | 0 · 1 · 3 (C40) | - |
| recorder allowlist (8 headers + 1 credential) | table in C10 | - |
| startup config: tracer env (2 assemblies) | compose generator C25 · e2e run C27 | - |
| report tiers rendered (O0, O1, O1v, O2, rung2) | table in C8 | - |
| K scenarios (12 variants) | C14 k0 · C27 k1 · C30 k2-dd-pre · C30b k2-dd-post · C31 k2-otel · C36 k3 · C42 k4 · C44 k5 · C46 k6 · C49 k7 x3 | - |

- Claims naming a status code, route or response shape: C9, C10, C11, C12 - each proof crosses the HTTP or CLI boundary
- C42 deliberately does not assert N grows with rate: live timing makes it non-deterministic; the distribution is reported

## Handoff

Greenfield, so sizes are estimates of what each slice writes plus the proto package it reads. Batches cut where the surface changes:

- **B1: S1, S2** (~20k) - offline harness only, `cmd/harness` + `internal/{scenario,oracle,report}`
- **B2: S3** (~40k + first docker/tracer iteration) - services, stack composer, Java app, e2e harness
- **B3: S4, S5** (~40k) - decoder and O1 truth, then OTel launch; both read the decoder
- **B4: S6, S7, S8** (~45k) - bridge, K3/O2, fixtures: the M0 exit criterion
- **B5: S9, S10** (~25k) - rung-2 truth and skew, same oracle surface
- **B6: S11** (~30k) - Go app, new module
- **B7: S12** (~5k) - record all sets, tag

### After B1

- Boundary: C1-C9 closed at 6d9302a (loader 3f9c7bd); `run`/`record` return "not implemented" (exit 1); O1, O1v, O2, rung2 are placeholders `{"verdict":"REPORTED","truth":{}}` for B3+ to fill; C35's negative `window_ms` rejection already exists, its test case does not.
- Settled mid-build: `harness report` exits 0 whenever report.json is written, whatever the verdict (1 error, 2 usage); a run dir without `ledger/` is an error, an empty ledger is vacuously PASS (no traffic-based expected seqs yet); ledger rows with an unknown `side` are an error; a missing writer is reported as `side:"producer"` even when the expected writer is a bridge, with `service` filled only when exactly one topology entry writes that destination; LedgerRow Go type lives in `internal/ledger` for the collector to reuse.
- Abandoned: recording empty objects/arrays as diff leaves - produced `{"path":"missing","a":[],"b":null}` noise next to the real `missing[0].seq` change.

### After B2

- Boundary: C10-C18 closed at `9316196` (services, ledger collector, O0 traffic seqs) plus the B2 commit that adds the stack composer, the Java app, the `run` subcommand, `scenarios/k0.yaml` and `e2e/TestK0`. K0 runs end to end in baseline mode: O0 PASS, 200 producer + 200 consumer rows, 6 APM payloads at the recorder, verdict PASS.
- Settled mid-build: the run's compose network is `internal: true`, which is what actually stops egress (the per-feature `DD_*_ENABLED` flags do not suppress the agent's startup EvP endpoint validation; my first probe missed it because it only watched 45 s). `DD_DD_URL` does not catch EvP tracks. The full agent image always starts the process agent and data plane, so they are pointed at the fake metrics sink. `go test -tags e2e` must be run with `-count=1`: the harness is a subprocess, so Go's cache cannot see a compose change.
- Facts for B3's decoder (measured on dd-java 1.66.0 + agent 7.83.2, zstd bodies):
  - `trace.Span.TraceID` is the low 64 bits and `Meta["_dd.p.tid"]` the high 64; the ledger's 128-bit ID matched captures exactly.
  - The numeric tags live in `Span.Metrics`, not `Meta`: the Kafka consumer carries `#partition`, `#offset` and `#record.queue_time_ms`, and the producer carries `#partition` and `#offset`. A decoder that reads only `Meta` sees no join keys at all.
  - **dd-java 1.66.0 records partition and offset on the Kafka producer span**, which the design says it does not (AI §6.3's upstream PR appears to be in). K2's dd-java variant is therefore repairable by rung 1, not "rung 2 only" as issue #6 expects - raised with the user before B4.
  - The ledger's producer span is the app's `akt.send` wrapper and the consumer's is `kafka.consume`; the messaging-producer span (`kafka.produce`) is a child of `akt.send`, so "the true producer span" is that child. Healthy propagation puts `kafka.consume`'s parent at the producer's `kafka.produce`: 0 dangling parents on K0.
  - Captures also contain spans outside the ledger (the consumer's own ledger POSTs appear as `http.request` spans), so the oracle must key on ledger rows rather than assume every captured span is one.
  - The agent does not emit the `idx` variant for these tracers yet, so C19's idx path needs a hand-built fixture rather than a K0 capture.
  - One trace of 200 (seq 114, both sides) never reached the recorder, with no tracer or agent drop logged, at sampling priority 2 throughout. Treat it as the uncaptured case C23 counts rather than as an orphan, and expect a small non-zero `uncaptured` on live runs.
- Abandoned: per-feature agent disable flags as the egress fix (kept only where they cut work); a standalone 45 s agent probe as evidence (too short to see the startup validation).

### After B3

- Boundary: C19-C23 closed at the B3 commit (decoder, capture reader, O1/O1v truth). C24 is **not built** - see below.
- C24 asks for `uncaptured == 0` on K0, and that is not a property the bench can assert: across five K0 runs, four had `uncaptured = 0` and one lost a whole trace (both its producer and its consumer span, seq 114, sampling priority 2 throughout, no tracer or agent drop logged). The tracer's own stats payloads for that run report 200 `akt.send` and 200 `kafka.produce` spans, so the loss is between the tracer and the recorder, not in the app. The proposal is to assert `O1.truth.total == 0` (the control claim) and leave `uncaptured` as a reported number. Waiting on the user.
- Facts recorded: `/api/v0.2/stats` bodies are **msgpack** (`Content-Type: application/msgpack`), not protobuf, so a stats decoder needs the generated `UnmarshalMsg`; M0/M1 do not decode stats at all. The agent sends 6 trace payloads for a 200-message K0 run, zstd-encoded, and never the `idx` variant, so C19's idx path is proven from a hand-built fixture.
- Settled mid-build: `oracle.Truth` takes the O0 result because the true producer of a read row is O0's bridge-aware answer, not the reader's own service. `repair` reads the producer span as the producer-kind child of the ledger producer span.
- Abandoned: `TestDiffPerTier`'s assertion that only O0 differs between two runs - C9 never claimed that, and it now fails correctly because O1v volume really does differ. The test asserts instead that a tier with no difference is omitted, which is what C9 does say.
