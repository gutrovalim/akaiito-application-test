package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type row struct {
	seq       int64
	side      string
	service   string
	dest      string
	span      string
	parentSeq int64
}

const k0Scenario = `id: k0
broker: kafka
topology:
  - {role: producer, lang: java, tracer: dd-java, version: "1.66.0", topic: orders}
  - {role: consumer, lang: java, tracer: dd-java, version: "1.66.0", topic: orders}
traffic: {messages: 2, rate_per_s: 10}
expect: {O0: PASS}
`

const stackJSON = `{"agent_version":"7.83.2","kafka_version":"4.3.1","tracers":[{"service":"akt-producer","tracer":"dd-java","version":"1.66.0"},{"service":"akt-consumer","tracer":"dd-java","version":"1.66.0"}]}`

func k0Rows(producerSeqs, consumerSeqs []int64) []row {
	var rs []row
	for _, s := range producerSeqs {
		rs = append(rs, row{seq: s, side: "producer", service: "akt-producer", dest: "orders"})
	}
	for _, s := range consumerSeqs {
		rs = append(rs, row{seq: s, side: "consumer", service: "akt-consumer", dest: "orders"})
	}
	return rs
}

func hexID(n int, parts ...any) string {
	h := fnv.New64a()
	fmt.Fprint(h, parts...)
	s := fmt.Sprintf("%016x", h.Sum64())
	return strings.Repeat(s, 2)[:n]
}

// writeRun builds a run dir <tmp>/<runID> holding scenario.yaml, stack.json and ledger/<service>.jsonl.
func writeRun(t *testing.T, runID, scenarioYAML string, rows []row) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), runID)
	if err := os.MkdirAll(filepath.Join(dir, "ledger"), 0o755); err != nil {
		t.Fatal(err)
	}
	must(t, os.WriteFile(filepath.Join(dir, "scenario.yaml"), []byte(scenarioYAML), 0o644))
	must(t, os.WriteFile(filepath.Join(dir, "stack.json"), []byte(stackJSON), 0o644))
	files := map[string]*bytes.Buffer{}
	for i, r := range rows {
		span := r.span
		if span == "" {
			span = hexID(16, r.service, r.side, r.seq)
		}
		m := map[string]any{
			"run_id": runID, "scenario_id": "k0", "seq": r.seq, "side": r.side, "service": r.service,
			"language": "java", "tracer": "dd-java", "tracer_version": "1.66.0", "broker": "kafka",
			"destination": r.dest, "native_id": map[string]any{"partition": 0, "offset": i},
			"trace_id": hexID(32, "trace", r.seq), "span_id": span, "ts_ns": 1_790_000_000_000_000_000 + int64(i),
		}
		if r.side == "bridge_out" {
			m["parent_seq"] = r.parentSeq
		}
		b, err := json.Marshal(m)
		must(t, err)
		if files[r.service] == nil {
			files[r.service] = &bytes.Buffer{}
		}
		files[r.service].Write(append(b, '\n'))
	}
	for svc, b := range files {
		must(t, os.WriteFile(filepath.Join(dir, "ledger", svc+".jsonl"), b.Bytes(), 0o644))
	}
	return dir
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func harness(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	var out, errb bytes.Buffer
	code := Main(args, &out, &errb)
	return code, out.String(), errb.String()
}

// runReport runs `harness report dir`, requires exit 0 and returns the parsed report.json.
func runReport(t *testing.T, dir string) map[string]any {
	t.Helper()
	if code, _, stderr := harness(t, "report", dir); code != 0 {
		t.Fatalf("report exit %d: %s", code, stderr)
	}
	return readJSON(t, filepath.Join(dir, "report.json"))
}

func readJSON(t *testing.T, path string) map[string]any {
	t.Helper()
	b, err := os.ReadFile(path)
	must(t, err)
	var m map[string]any
	must(t, json.Unmarshal(b, &m))
	return m
}

// get walks a parsed JSON document by object keys.
func get(t *testing.T, v any, keys ...string) any {
	t.Helper()
	for _, k := range keys {
		m, ok := v.(map[string]any)
		if !ok {
			t.Fatalf("at %q: not an object: %v", k, v)
		}
		if v, ok = m[k]; !ok {
			t.Fatalf("key %q absent in %v", k, m)
		}
	}
	return v
}
