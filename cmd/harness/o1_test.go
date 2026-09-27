package main

import (
	"bytes"
	"compress/gzip"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/DataDog/datadog-agent/pkg/proto/pbgo/trace"
	"github.com/DataDog/datadog-agent/pkg/proto/pbgo/trace/idx"
	"github.com/gutrovalim/akaiito-application-test/internal/services"
	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/proto"
)

// spanSpec describes one span to put in a hand-built capture.
type spanSpec struct {
	traceID           string // 32 or 16 hex; the low 64 bits go on the wire
	spanID            string // 16 hex
	parentID          string // 16 hex, empty means a trace root
	service           string
	name              string
	kind              string // span.kind
	partition, offset *int64
	queueMS           *int64
	start, duration   int64
}

func id64(t *testing.T, hexID string) uint64 {
	t.Helper()
	v, err := strconv.ParseUint(hexID[len(hexID)-16:], 16, 64)
	must(t, err)
	return v
}

// payload builds an AgentPayload of the requested variant holding these spans.
func payload(t *testing.T, variant string, spans []spanSpec) *trace.AgentPayload {
	t.Helper()
	if variant == "idx" {
		stringsTable := []string{""}
		ref := func(s string) uint32 {
			for i, v := range stringsTable {
				if v == s {
					return uint32(i)
				}
			}
			stringsTable = append(stringsTable, s)
			return uint32(len(stringsTable) - 1)
		}
		var idxSpans []*idx.Span
		var traceID []byte
		for _, s := range spans {
			b, err := hex.DecodeString(s.traceID)
			must(t, err)
			if len(b) == 8 {
				b = append(make([]byte, 8), b...)
			}
			traceID = b
			attrs := map[uint32]*idx.AnyValue{}
			if s.kind != "" {
				attrs[ref("span.kind")] = &idx.AnyValue{Value: &idx.AnyValue_StringValueRef{StringValueRef: ref(s.kind)}}
			}
			for key, v := range map[string]*int64{"partition": s.partition, "offset": s.offset, "record.queue_time_ms": s.queueMS} {
				if v != nil {
					attrs[ref(key)] = &idx.AnyValue{Value: &idx.AnyValue_IntValue{IntValue: *v}}
				}
			}
			idxSpans = append(idxSpans, &idx.Span{
				ServiceRef:  ref(s.service),
				NameRef:     ref(s.name),
				ResourceRef: ref(s.name),
				SpanID:      id64(t, s.spanID),
				ParentID:    parentID(t, s.parentID),
				Start:       uint64(s.start),
				Duration:    uint64(s.duration),
				Attributes:  attrs,
			})
		}
		return &trace.AgentPayload{IdxTracerPayloads: []*idx.TracerPayload{{
			Strings: stringsTable,
			Chunks:  []*idx.TraceChunk{{TraceID: traceID, Spans: idxSpans}},
		}}}
	}
	var out []*trace.Span
	for _, s := range spans {
		meta := map[string]string{}
		if s.kind != "" {
			meta["span.kind"] = s.kind
		}
		if len(s.traceID) > 16 {
			meta["_dd.p.tid"] = s.traceID[:len(s.traceID)-16]
		}
		metrics := map[string]float64{}
		for key, v := range map[string]*int64{"partition": s.partition, "offset": s.offset, "record.queue_time_ms": s.queueMS} {
			if v != nil {
				metrics[key] = float64(*v)
			}
		}
		out = append(out, &trace.Span{
			Service:  s.service,
			Name:     s.name,
			Resource: s.name,
			TraceID:  id64(t, s.traceID),
			SpanID:   id64(t, s.spanID),
			ParentID: parentID(t, s.parentID),
			Start:    s.start,
			Duration: s.duration,
			Meta:     meta,
			Metrics:  metrics,
		})
	}
	return &trace.AgentPayload{TracerPayloads: []*trace.TracerPayload{{
		Chunks: []*trace.TraceChunk{{Spans: out}},
	}}}
}

func parentID(t *testing.T, hexID string) uint64 {
	t.Helper()
	if hexID == "" {
		return 0
	}
	return id64(t, hexID)
}

// writeCapture encodes a payload the way the agent would and stores it as a recorder capture.
func writeCapture(t *testing.T, runDir string, seq int64, enc, variant string, spans []spanSpec) {
	t.Helper()
	raw, err := proto.Marshal(payload(t, variant, spans))
	must(t, err)
	body := encode(t, raw, enc)
	dir := filepath.Join(runDir, "recorder")
	must(t, os.MkdirAll(dir, 0o755))
	meta := services.Meta{
		Seq: seq, TsNS: 1_790_000_000_000_000_000 + seq,
		Method: "POST", Path: "/api/v0.2/traces",
		Headers: map[string]string{"Content-Encoding": enc, "Content-Type": "application/x-protobuf"},
	}
	mb, err := json.Marshal(meta)
	must(t, err)
	base := filepath.Join(dir, fmt.Sprintf("%06d", seq))
	must(t, os.WriteFile(base+".meta.json", mb, 0o644))
	must(t, os.WriteFile(base+".body", body, 0o644))
}

func encode(t *testing.T, raw []byte, enc string) []byte {
	t.Helper()
	var buf bytes.Buffer
	switch enc {
	case "", "identity":
		return raw
	case "gzip":
		w := gzip.NewWriter(&buf)
		_, err := w.Write(raw)
		must(t, err)
		must(t, w.Close())
	case "zstd":
		w, err := zstd.NewWriter(&buf)
		must(t, err)
		_, err = w.Write(raw)
		must(t, err)
		must(t, w.Close())
	default:
		t.Fatalf("unknown encoding %q", enc)
	}
	return buf.Bytes()
}

func i64(v int64) *int64 { return &v }

// C19: every Content-Encoding and both tracer-payload variants decode.
func TestReportDecodesEncodingsAndIdx(t *testing.T) {
	for _, variant := range []string{"plain", "idx"} {
		for _, enc := range []string{"identity", "gzip", "zstd"} {
			t.Run(variant+"/"+enc, func(t *testing.T) {
				rows := k0Rows([]int64{1}, []int64{1})
				dir := writeRun(t, "k0-20260927T000000Z-c001", k0Scenario, rows)
				// The consumer row's span is a trace root, so it is a certain orphan.
				writeCapture(t, dir, 1, enc, variant, []spanSpec{{
					traceID: hexID(32, "trace", 1), spanID: hexID(16, "akt-consumer", "consumer", 1),
					service: "akt-consumer", name: "kafka.consume", kind: "consumer",
				}})
				r := runReport(t, dir)
				if got := get(t, r, "tiers", "O1", "truth", "total"); got != float64(1) {
					t.Errorf("O1 total = %v, want 1", got)
				}
				if got := get(t, r, "tiers", "O1", "truth", "uncaptured"); got != float64(0) {
					t.Errorf("O1 uncaptured = %v, want 0 (the payload did not decode)", got)
				}
			})
		}
	}
}

// C20: the 128-bit rule joins the ledger id to the captured one.
func TestReportTraceID128Rule(t *testing.T) {
	const lowHalf = "275914e08bff6537"
	cases := []struct {
		name        string
		ledgerTrace string
		captured    string
		want        float64
	}{
		{"high and low match", "6ab91e5600000000" + lowHalf, "6ab91e5600000000" + lowHalf, 0},
		{"no tid, low only", lowHalf, lowHalf, 0},
		{"same low, different tid", "6ab91e5600000000" + lowHalf, "6ab91e5a00000000" + lowHalf, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rows := k0Rows([]int64{1}, []int64{1})
			dir := writeRun(t, "k0-20260927T000000Z-c002", k0Scenario, rows)
			// Rewrite the consumer ledger row with the case's trace id.
			rewriteTraceID(t, dir, "akt-consumer", tc.ledgerTrace)
			writeCapture(t, dir, 1, "zstd", "plain", []spanSpec{{
				traceID: tc.captured, spanID: hexID(16, "akt-consumer", "consumer", 1),
				service: "akt-consumer", name: "kafka.consume", kind: "consumer",
			}})
			r := runReport(t, dir)
			if got := get(t, r, "tiers", "O1", "truth", "trace_id_mismatch"); got != tc.want {
				t.Errorf("trace_id_mismatch = %v, want %v", got, tc.want)
			}
		})
	}
}

// C21: certain orphans are counted per destination x service x broker, split by rung.
func TestReportO1CertainOrphansRepairableSplit(t *testing.T) {
	rows := k0Rows([]int64{1, 2, 3, 4}, []int64{1, 2, 3, 4})
	dir := writeRun(t, "k0-20260927T000000Z-c003", k0Scenario, rows)
	producerSpan := func(seq int64) string { return hexID(16, "akt-producer", "producer", seq) }
	consumerSpan := func(seq int64) string { return hexID(16, "akt-consumer", "consumer", seq) }

	// seq 1: rung 1, the orphan consumer span and the producer span carry the same native id.
	writeCapture(t, dir, 1, "zstd", "plain", []spanSpec{
		{traceID: hexID(32, "trace", 1), spanID: producerSpan(1), service: "akt-producer", name: "akt.send"},
		{traceID: hexID(32, "trace", 1), spanID: hexID(16, "produce", 1), parentID: producerSpan(1), service: "kafka", name: "kafka.produce", kind: "producer", partition: i64(2), offset: i64(0), start: 1000, duration: 100},
		{traceID: hexID(32, "trace", 1), spanID: consumerSpan(1), service: "akt-consumer", name: "kafka.consume", kind: "consumer", partition: i64(2), offset: i64(0)},
	})
	// seq 2: rung 2, no native id on the consumer, but the queue time derives a timestamp
	// inside the producer span's window.
	writeCapture(t, dir, 2, "zstd", "plain", []spanSpec{
		{traceID: hexID(32, "trace", 2), spanID: producerSpan(2), service: "akt-producer", name: "akt.send"},
		{traceID: hexID(32, "trace", 2), spanID: hexID(16, "produce", 2), parentID: producerSpan(2), service: "kafka", name: "kafka.produce", kind: "producer", start: 5_000_000_000, duration: 100_000_000},
		{traceID: hexID(32, "trace", 2), spanID: consumerSpan(2), service: "akt-consumer", name: "kafka.consume", kind: "consumer", queueMS: i64(30), start: 5_050_000_000},
	})
	// seq 3: no join key at all.
	writeCapture(t, dir, 3, "zstd", "plain", []spanSpec{
		{traceID: hexID(32, "trace", 3), spanID: producerSpan(3), service: "akt-producer", name: "akt.send"},
		{traceID: hexID(32, "trace", 3), spanID: consumerSpan(3), service: "akt-consumer", name: "kafka.consume", kind: "consumer"},
	})
	// seq 4: healthy, the consumer span has a parent, so it is no orphan.
	writeCapture(t, dir, 4, "zstd", "plain", []spanSpec{
		{traceID: hexID(32, "trace", 4), spanID: producerSpan(4), service: "akt-producer", name: "akt.send"},
		{traceID: hexID(32, "trace", 4), spanID: hexID(16, "produce", 4), parentID: producerSpan(4), service: "kafka", name: "kafka.produce", kind: "producer", partition: i64(1), offset: i64(7)},
		{traceID: hexID(32, "trace", 4), spanID: consumerSpan(4), parentID: hexID(16, "produce", 4), service: "akt-consumer", name: "kafka.consume", kind: "consumer", partition: i64(1), offset: i64(7)},
	})

	r := runReport(t, dir)
	if got := get(t, r, "tiers", "O1", "truth", "total"); got != float64(3) {
		t.Errorf("O1 total = %v, want 3", got)
	}
	groups := get(t, r, "tiers", "O1", "truth", "groups").([]any)
	if len(groups) != 1 {
		t.Fatalf("groups = %d, want 1: %v", len(groups), groups)
	}
	g := groups[0]
	for key, want := range map[string]any{
		"destination": "orders", "service": "akt-consumer", "broker": "kafka",
		"total": float64(3), "rung1": float64(1), "rung2": float64(1), "not_repairable": float64(1),
	} {
		if got := get(t, g, key); got != want {
			t.Errorf("group %s = %v, want %v", key, got, want)
		}
	}
}

// C22: O1v counts producer and consumer volume, and a redelivery is its own consumer span.
func TestReportO1vVolumeCountsRedeliveries(t *testing.T) {
	rows := []row{
		{seq: 1, side: "producer", service: "akt-producer", dest: "orders"},
		{seq: 2, side: "producer", service: "akt-producer", dest: "orders"},
		{seq: 1, side: "consumer", service: "akt-consumer", dest: "orders", span: hexID(16, "first", 1)},
		{seq: 2, side: "consumer", service: "akt-consumer", dest: "orders", span: hexID(16, "first", 2)},
		// a redelivery: same seq and service, a different span.
		{seq: 2, side: "consumer", service: "akt-consumer", dest: "orders", span: hexID(16, "redelivery", 2)},
	}
	dir := writeRun(t, "k0-20260927T000000Z-c004", k0Scenario, rows)
	r := runReport(t, dir)
	if got := get(t, r, "tiers", "O0", "deduped"); got != float64(0) {
		t.Errorf("O0 deduped = %v, want 0 (a redelivery is not a duplicate)", got)
	}
	if got := get(t, r, "tiers", "O0", "verdict"); got != "PASS" {
		t.Errorf("O0 verdict = %v, want PASS", got)
	}
	groups := get(t, r, "tiers", "O1v", "truth", "groups").([]any)
	if len(groups) != 2 {
		t.Fatalf("groups = %d, want 2: %v", len(groups), groups)
	}
	if got := get(t, groups[0], "service"); got != "akt-consumer" {
		t.Fatalf("groups[0].service = %v, want akt-consumer", got)
	}
	if got := get(t, groups[0], "consumers"); got != float64(3) {
		t.Errorf("consumer volume = %v, want 3", got)
	}
	if got := get(t, groups[0], "producers"); got != float64(0) {
		t.Errorf("consumer group producers = %v, want 0", got)
	}
	if got := get(t, groups[1], "producers"); got != float64(2) {
		t.Errorf("producer volume = %v, want 2", got)
	}
}

// C23: a ledger row whose span never reached the recorder is uncaptured, not an orphan.
func TestReportO1Uncaptured(t *testing.T) {
	rows := k0Rows([]int64{1, 2}, []int64{1, 2})
	dir := writeRun(t, "k0-20260927T000000Z-c005", k0Scenario, rows)
	// Only seq 1 reaches the recorder, and as a healthy child span.
	writeCapture(t, dir, 1, "zstd", "plain", []spanSpec{
		{traceID: hexID(32, "trace", 1), spanID: hexID(16, "akt-producer", "producer", 1), service: "akt-producer", name: "akt.send"},
		{traceID: hexID(32, "trace", 1), spanID: hexID(16, "produce", 1), parentID: hexID(16, "akt-producer", "producer", 1), service: "kafka", name: "kafka.produce", kind: "producer"},
		{traceID: hexID(32, "trace", 1), spanID: hexID(16, "akt-consumer", "consumer", 1), parentID: hexID(16, "produce", 1), service: "akt-consumer", name: "kafka.consume", kind: "consumer"},
	})
	r := runReport(t, dir)
	if got := get(t, r, "tiers", "O1", "truth", "uncaptured"); got != float64(1) {
		t.Errorf("uncaptured = %v, want 1", got)
	}
	if got := get(t, r, "tiers", "O1", "truth", "total"); got != float64(0) {
		t.Errorf("O1 total = %v, want 0 (an uncaptured row is not an orphan)", got)
	}
}

// rewriteTraceID replaces the trace_id of every row of one service's ledger file.
func rewriteTraceID(t *testing.T, runDir, service, traceID string) {
	t.Helper()
	path := filepath.Join(runDir, "ledger", service+".jsonl")
	b, err := os.ReadFile(path)
	must(t, err)
	var out bytes.Buffer
	for _, line := range bytes.Split(bytes.TrimSpace(b), []byte("\n")) {
		var m map[string]any
		must(t, json.Unmarshal(line, &m))
		m["trace_id"] = traceID
		enc, err := json.Marshal(m)
		must(t, err)
		out.Write(append(enc, '\n'))
	}
	must(t, os.WriteFile(path, out.Bytes(), 0o644))
}
