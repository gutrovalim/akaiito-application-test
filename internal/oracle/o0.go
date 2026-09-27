package oracle

import (
	"cmp"
	"fmt"
	"slices"

	"github.com/gutrovalim/akaiito-application-test/internal/ledger"
	"github.com/gutrovalim/akaiito-application-test/internal/scenario"
)

const (
	Pass     = "PASS"
	Fail     = "FAIL"
	Invalid  = "INVALID"
	Reported = "REPORTED"
)

type Missing struct {
	Seq         int64  `json:"seq"`
	Side        string `json:"side"`
	Service     string `json:"service,omitempty"`
	Destination string `json:"destination,omitempty"`
}

type O0 struct {
	Verdict       string            `json:"verdict"`
	Missing       []Missing         `json:"missing"`
	Deduped       int               `json:"deduped"`
	TrueProducers map[string]string `json:"true_producers"`
}

// Validity runs the O0 gate: every seq read or re-produced has a writer row, and every
// entry subscribed to a destination has a read row for each seq written to it.
// It returns the deduped ledger alongside the verdict.
func Validity(s *scenario.Scenario, rows []ledger.Row) (O0, []ledger.Row) {
	type readKey struct {
		seq           int64
		service, span string
	}
	type writer struct{ service, dest string }
	o := O0{Missing: []Missing{}, TrueProducers: map[string]string{}}
	seen := map[readKey]bool{}
	writers := map[int64]writer{}
	var kept []ledger.Row
	for _, r := range rows {
		switch r.Side {
		case ledger.Consumer, ledger.BridgeIn:
			k := readKey{r.Seq, r.Service, r.SpanID}
			if seen[k] {
				o.Deduped++
				continue
			}
			seen[k] = true
		case ledger.Producer, ledger.BridgeOut:
			if _, ok := writers[r.Seq]; !ok {
				writers[r.Seq] = writer{r.Service, r.Destination}
			}
		}
		kept = append(kept, r)
	}

	read := map[string]bool{}
	missingWriter := map[int64]string{}
	need := func(seq int64, dest string) {
		if _, ok := writers[seq]; ok {
			return
		}
		if d, ok := missingWriter[seq]; !ok || d == "" {
			missingWriter[seq] = dest
		}
	}
	for _, r := range kept {
		switch r.Side {
		case ledger.Consumer, ledger.BridgeIn:
			read[fmt.Sprintf("%s/%s/%d", r.Side, r.Service, r.Seq)] = true
			need(r.Seq, r.Destination)
			if w, ok := writers[r.Seq]; ok {
				o.TrueProducers[fmt.Sprintf("%s/%d", r.Service, r.Seq)] = w.service
			}
		case ledger.BridgeOut:
			if r.ParentSeq != nil {
				need(*r.ParentSeq, "")
			}
		}
	}
	for seq, dest := range missingWriter {
		m := Missing{Seq: seq, Side: ledger.Producer, Destination: dest}
		var ws []string
		for _, e := range s.Topology {
			if dest != "" && (e.Role == scenario.Producer && e.Topic == dest || e.Role == scenario.Bridge && e.To == dest) {
				ws = append(ws, e.Service)
			}
		}
		if len(ws) == 1 {
			m.Service = ws[0]
		}
		o.Missing = append(o.Missing, m)
	}
	for seq, w := range writers {
		for _, e := range s.Topology {
			if e.Subscribes() == "" || e.Subscribes() != w.dest {
				continue
			}
			if side := e.ReadSide(); !read[fmt.Sprintf("%s/%s/%d", side, e.Service, seq)] {
				o.Missing = append(o.Missing, Missing{Seq: seq, Side: side, Service: e.Service, Destination: w.dest})
			}
		}
	}
	slices.SortFunc(o.Missing, func(a, b Missing) int {
		return cmp.Or(cmp.Compare(a.Seq, b.Seq), cmp.Compare(a.Side, b.Side), cmp.Compare(a.Service, b.Service))
	})
	o.Verdict = Pass
	if len(o.Missing) > 0 {
		o.Verdict = Invalid
	}
	return o, kept
}
