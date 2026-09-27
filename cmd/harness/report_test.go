package main

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const runID = "k0-20260926T120000Z-ab12"

func missingEntries(t *testing.T, rep map[string]any) []map[string]any {
	t.Helper()
	raw, ok := get(t, rep, "tiers", "O0", "missing").([]any)
	if !ok {
		t.Fatalf("tiers.O0.missing is not an array")
	}
	var out []map[string]any
	for _, m := range raw {
		out = append(out, m.(map[string]any))
	}
	return out
}

func TestReportO0CompletePass(t *testing.T) {
	dir := writeRun(t, runID, k0Scenario, k0Rows([]int64{1, 2, 3}, []int64{1, 2, 3}))
	rep := runReport(t, dir)
	for key, want := range map[string]any{"scenario_id": "k0", "run_id": runID, "verdict": "PASS"} {
		if got := get(t, rep, key); got != want {
			t.Errorf("%s = %v, want %v", key, got, want)
		}
	}
	if got := get(t, rep, "tiers", "O0", "verdict"); got != "PASS" {
		t.Errorf("tiers.O0.verdict = %v, want PASS", got)
	}
	if m := missingEntries(t, rep); len(m) != 0 {
		t.Errorf("tiers.O0.missing = %v, want empty", m)
	}
}

func TestReportO0MissingProducerInvalid(t *testing.T) {
	dir := writeRun(t, runID, k0Scenario, k0Rows([]int64{1, 3}, []int64{1, 2, 3}))
	rep := runReport(t, dir)
	if got := get(t, rep, "verdict"); got != "INVALID" {
		t.Errorf("verdict = %v, want INVALID", got)
	}
	if got := get(t, rep, "tiers", "O0", "verdict"); got != "INVALID" {
		t.Errorf("tiers.O0.verdict = %v, want INVALID", got)
	}
	m := missingEntries(t, rep)
	if len(m) != 1 || m[0]["seq"] != 2.0 || m[0]["side"] != "producer" {
		t.Errorf("tiers.O0.missing = %v, want one {seq:2, side:producer}", m)
	}
}

func TestReportO0FanOutMissingConsumerInvalid(t *testing.T) {
	sc := `id: k0-fanout
broker: kafka
topology:
  - {role: producer, lang: java, tracer: dd-java, version: "1.66.0", topic: orders}
  - {name: billing, role: consumer, lang: java, tracer: dd-java, version: "1.66.0", topic: orders}
  - {name: audit, role: consumer, lang: java, tracer: dd-java, version: "1.66.0", topic: orders}
`
	rows := []row{
		{seq: 1, side: "producer", service: "akt-producer", dest: "orders"},
		{seq: 2, side: "producer", service: "akt-producer", dest: "orders"},
		{seq: 1, side: "consumer", service: "billing", dest: "orders"},
		{seq: 2, side: "consumer", service: "billing", dest: "orders"},
		{seq: 1, side: "consumer", service: "audit", dest: "orders"},
	}
	rep := runReport(t, writeRun(t, runID, sc, rows))
	if got := get(t, rep, "verdict"); got != "INVALID" {
		t.Errorf("verdict = %v, want INVALID", got)
	}
	if got := get(t, rep, "tiers", "O0", "verdict"); got != "INVALID" {
		t.Errorf("tiers.O0.verdict = %v, want INVALID", got)
	}
	m := missingEntries(t, rep)
	if len(m) != 1 || m[0]["seq"] != 2.0 || m[0]["service"] != "audit" {
		t.Errorf("tiers.O0.missing = %v, want one entry naming audit and seq 2", m)
	}
}

func TestReportO0DedupAndRedelivery(t *testing.T) {
	cases := []struct {
		name        string
		consumer    []row
		wantDeduped float64
	}{
		{"duplicates", []row{
			{seq: 1, span: "00000000000000a1"},
			{seq: 1, span: "00000000000000a1"},
			{seq: 1, span: "00000000000000a1"},
			{seq: 2, span: "00000000000000b1"},
		}, 2},
		{"redelivery", []row{
			{seq: 1, span: "00000000000000a1"},
			{seq: 2, span: "00000000000000b1"},
			{seq: 2, span: "00000000000000b2"},
		}, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rows := k0Rows([]int64{1, 2}, nil)
			for _, c := range tc.consumer {
				c.side, c.service, c.dest = "consumer", "akt-consumer", "orders"
				rows = append(rows, c)
			}
			rep := runReport(t, writeRun(t, runID, k0Scenario, rows))
			if got := get(t, rep, "tiers", "O0", "verdict"); got != "PASS" {
				t.Errorf("tiers.O0.verdict = %v, want PASS", got)
			}
			if got := get(t, rep, "tiers", "O0", "deduped"); got != tc.wantDeduped {
				t.Errorf("tiers.O0.deduped = %v, want %v", got, tc.wantDeduped)
			}
		})
	}
}

func TestReportO0BridgeChain(t *testing.T) {
	sc := `id: k2-dd-java
broker: kafka
topology:
  - {role: producer, lang: java, tracer: dd-java, version: "1.66.0", topic: orders}
  - {name: copier, role: bridge, lang: java, tracer: dd-java, version: "1.66.0", from: orders, to: orders-copy, forward_headers: false}
  - {role: consumer, lang: java, tracer: dd-java, version: "1.66.0", topic: orders-copy}
`
	rows := []row{
		{seq: 1, side: "producer", service: "akt-producer", dest: "orders"},
		{seq: 1, side: "bridge_in", service: "copier", dest: "orders"},
		{seq: 2, side: "bridge_out", service: "copier", dest: "orders-copy", parentSeq: 1},
		{seq: 2, side: "consumer", service: "akt-consumer", dest: "orders-copy"},
	}
	rep := runReport(t, writeRun(t, runID, sc, rows))
	if got := get(t, rep, "tiers", "O0", "verdict"); got != "PASS" {
		t.Errorf("tiers.O0.verdict = %v, want PASS", got)
	}
	if got := get(t, rep, "tiers", "O0", "true_producers", "akt-consumer/2"); got != "copier" {
		t.Errorf(`tiers.O0.true_producers["akt-consumer/2"] = %v, want copier`, got)
	}
}

func TestReportRejectsInvalidScenario(t *testing.T) {
	cases := []struct {
		name, scenario, want string
	}{
		{"unknown key", "id: bad\nbroker: kafka\ncolour: red\ntopology:\n  - {role: producer, topic: orders}\n", "colour"},
		{"missing id", "broker: kafka\ntopology:\n  - {role: producer, topic: orders}\n", "missing id"},
		{"unknown role", "id: bad\nbroker: kafka\ntopology:\n  - {role: producer, topic: orders}\n  - {role: sink, topic: orders}\n", `unknown role "sink"`},
		{"consumer without topic", "id: bad\nbroker: kafka\ntopology:\n  - {role: producer, topic: orders}\n  - {role: consumer}\n", "missing topic"},
		{"bridge without from/to", "id: bad\nbroker: kafka\ntopology:\n  - {role: producer, topic: orders}\n  - {role: bridge, from: orders}\n", "missing from/to"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := writeRun(t, runID, tc.scenario, k0Rows([]int64{1}, []int64{1}))
			code, _, stderr := harness(t, "report", dir)
			if code == 0 {
				t.Errorf("exit 0, want non-zero")
			}
			if !strings.Contains(stderr, tc.want) {
				t.Errorf("stderr %q does not name the problem %q", stderr, tc.want)
			}
			if _, err := os.Stat(filepath.Join(dir, "report.json")); !errors.Is(err, fs.ErrNotExist) {
				t.Errorf("report.json written (stat err %v)", err)
			}
		})
	}
}
