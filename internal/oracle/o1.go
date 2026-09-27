package oracle

import (
	"encoding/json"
	"slices"
	"strconv"
	"strings"

	"github.com/gutrovalim/akaiito-application-test/internal/captures"
	"github.com/gutrovalim/akaiito-application-test/internal/ledger"
	"github.com/gutrovalim/akaiito-application-test/internal/scenario"
	"github.com/gutrovalim/akaiito-application-test/internal/trace"
)

// Group is the certain-orphan count of one destination x service x broker.
type Group struct {
	Destination   string `json:"destination"`
	Service       string `json:"service"`
	Broker        string `json:"broker"`
	Total         int    `json:"total"`
	Rung1         int    `json:"rung1"`
	Rung2         int    `json:"rung2"`
	NotRepairable int    `json:"not_repairable"`
}

// O1Truth is the certain-orphan truth: consumer spans that are trace roots, joined to
// their true producer through the ledger and split by the ladder rung that could repair them.
type O1Truth struct {
	Total           int     `json:"total"`
	Groups          []Group `json:"groups"`
	Uncaptured      int     `json:"uncaptured"`
	TraceIDMismatch int     `json:"trace_id_mismatch"`
}

// VolumeGroup is the producer and consumer span volume of one destination x service x broker.
type VolumeGroup struct {
	Destination string `json:"destination"`
	Service     string `json:"service"`
	Broker      string `json:"broker"`
	Producers   int    `json:"producers"`
	Consumers   int    `json:"consumers"`
}

// O1vTruth is the volume truth.
type O1vTruth struct {
	Groups []VolumeGroup `json:"groups"`
}

// AsMap renders a truth struct as the report tier's truth object.
func AsMap(v any) map[string]any {
	b, err := json.Marshal(v)
	if err != nil {
		return map[string]any{}
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return map[string]any{}
	}
	return m
}

// Truth computes O1 and O1v from the deduped ledger and the recorder captures.
// The true producer of a read row comes from O0, which follows bridge chains to the
// last writer of that seq.
func Truth(s *scenario.Scenario, o0 O0, rows []ledger.Row, caps []captures.Capture) (O1Truth, O1vTruth) {
	index := indexSpans(caps)
	writers := map[string]ledger.Row{}
	for _, r := range rows {
		if r.Side == ledger.Producer || r.Side == ledger.BridgeOut {
			writers[writerKey(r.Service, r.Seq)] = r
		}
	}

	o1 := O1Truth{Groups: []Group{}}
	type groupKey struct{ dest, service string }
	counts := map[groupKey]*Group{}
	var groupOrder []groupKey
	for _, r := range rows {
		if r.Side != ledger.Consumer && r.Side != ledger.BridgeIn {
			continue
		}
		span, ok := index.byID[r.SpanID]
		if !ok {
			o1.Uncaptured++
			continue
		}
		if !traceIDMatches(r.TraceID, span.TraceID) {
			o1.TraceIDMismatch++
		}
		if !span.IsRoot() {
			continue
		}
		k := groupKey{r.Destination, r.Service}
		g, ok := counts[k]
		if !ok {
			g = &Group{Destination: r.Destination, Service: r.Service, Broker: s.Broker}
			counts[k] = g
			groupOrder = append(groupOrder, k)
		}
		g.Total++
		o1.Total++
		switch repair(s, r, o0, span, writers, index) {
		case "rung1":
			g.Rung1++
		case "rung2":
			g.Rung2++
		default:
			g.NotRepairable++
		}
	}
	for _, k := range groupOrder {
		o1.Groups = append(o1.Groups, *counts[k])
	}
	slices.SortFunc(o1.Groups, func(a, b Group) int {
		return compare(a.Destination, b.Destination, a.Service, b.Service, a.Broker, b.Broker)
	})
	return o1, volumeTruth(rows, s.Broker)
}

// volumeTruth counts producer and consumer rows per destination x service x broker.
// Redeliveries are distinct rows, so each counts as its own consumer span.
func volumeTruth(rows []ledger.Row, broker string) O1vTruth {
	type groupKey struct{ dest, service string }
	counts := map[groupKey]*VolumeGroup{}
	var order []groupKey
	for _, r := range rows {
		k := groupKey{r.Destination, r.Service}
		g, ok := counts[k]
		if !ok {
			g = &VolumeGroup{Destination: r.Destination, Service: r.Service, Broker: broker}
			counts[k] = g
			order = append(order, k)
		}
		switch r.Side {
		case ledger.Producer, ledger.BridgeOut:
			g.Producers++
		case ledger.Consumer, ledger.BridgeIn:
			g.Consumers++
		}
	}
	out := O1vTruth{Groups: []VolumeGroup{}}
	for _, k := range order {
		out.Groups = append(out.Groups, *counts[k])
	}
	slices.SortFunc(out.Groups, func(a, b VolumeGroup) int {
		return compare(a.Destination, b.Destination, a.Service, b.Service, a.Broker, b.Broker)
	})
	return out
}

// repair classifies one certain orphan by the ladder rung that could repair it:
// rung1 when the consumer and its true producer span carry the same native id,
// rung2 when the consumer's queue time derives a timestamp inside the producer span's window.
func repair(s *scenario.Scenario, row ledger.Row, o0 O0, consumer trace.Span, writers map[string]ledger.Row, index spanIndex) string {
	producerService, ok := o0.TrueProducers[writerKey(row.Service, row.Seq)]
	if !ok {
		return "not_repairable"
	}
	writer, ok := writers[writerKey(producerService, row.Seq)]
	if !ok {
		return "not_repairable"
	}
	producer, ok := index.messagingProducer(writer.SpanID)
	if !ok {
		return "not_repairable"
	}
	cPart, hasPart := consumer.Int("partition")
	cOffset, hasOffset := consumer.Int("offset")
	if hasPart && hasOffset {
		pPart, hasPart := producer.Int("partition")
		pOffset, hasOffset := producer.Int("offset")
		if hasPart && hasOffset && pPart == cPart && pOffset == cOffset {
			return "rung1"
		}
	}
	queue, ok := consumer.Int("record.queue_time_ms")
	if !ok {
		return "not_repairable"
	}
	window := int64(0)
	if s.Expect.Rung2 != nil {
		window = s.Expect.Rung2.CandidateWindowMS * 1_000_000
	}
	derived := consumer.Start - queue*1_000_000
	if producer.Start-window <= derived && derived <= producer.Start+producer.Duration+window {
		return "rung2"
	}
	return "not_repairable"
}

func writerKey(service string, seq int64) string {
	return service + "/" + strconv.FormatInt(seq, 10)
}

// traceIDMatches compares a ledger trace id with a captured one on the 128-bit rule:
// the low 64 bits must agree, and the high 64 bits too whenever the capture carries them
// (the wire trace id is 64-bit, with the high half in the _dd.p.tid tag).
func traceIDMatches(ledgerID, capturedID string) bool {
	l, c := strings.ToLower(ledgerID), strings.ToLower(capturedID)
	if l == "" || c == "" {
		return false
	}
	if low(l) != low(c) {
		return false
	}
	return len(c) <= 16 || len(l) <= 16 || high(l) == high(c)
}

func low(id string) string {
	if len(id) > 16 {
		return id[len(id)-16:]
	}
	return id
}

func high(id string) string {
	if len(id) > 16 {
		return id[:len(id)-16]
	}
	return ""
}

// spanIndex is every captured span, indexed for parent and producer lookups.
type spanIndex struct {
	byID     map[string]trace.Span
	children map[string][]trace.Span
}

func indexSpans(caps []captures.Capture) spanIndex {
	index := spanIndex{byID: map[string]trace.Span{}, children: map[string][]trace.Span{}}
	for _, c := range caps {
		for _, s := range c.Spans {
			if _, ok := index.byID[s.SpanID]; !ok {
				index.byID[s.SpanID] = s
			}
			index.children[s.ParentID] = append(index.children[s.ParentID], s)
		}
	}
	return index
}

// messagingProducer returns the messaging-producer span of a ledger producer span:
// its producer-kind child, or the ledger span itself when that already is one.
func (i spanIndex) messagingProducer(ledgerSpanID string) (trace.Span, bool) {
	for _, c := range i.children[ledgerSpanID] {
		if isKind(c, "producer") {
			return c, true
		}
	}
	if s, ok := i.byID[ledgerSpanID]; ok && isKind(s, "producer") {
		return s, true
	}
	return trace.Span{}, false
}

func isKind(s trace.Span, kind string) bool {
	k := strings.ToLower(s.Kind())
	return k == kind || strings.Contains(k, kind)
}

func compare(a1, b1, a2, b2, a3, b3 string) int {
	if c := strings.Compare(a1, b1); c != 0 {
		return c
	}
	if c := strings.Compare(a2, b2); c != 0 {
		return c
	}
	return strings.Compare(a3, b3)
}
