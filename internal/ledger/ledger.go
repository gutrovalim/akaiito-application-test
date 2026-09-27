package ledger

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

type NativeID struct {
	Partition *int32  `json:"partition,omitempty"`
	Offset    *int64  `json:"offset,omitempty"`
	MessageID *string `json:"message_id,omitempty"`
}

type Row struct {
	RunID         string    `json:"run_id"`
	ScenarioID    string    `json:"scenario_id"`
	Seq           int64     `json:"seq"`
	Side          string    `json:"side"`
	ParentSeq     *int64    `json:"parent_seq,omitempty"`
	Service       string    `json:"service"`
	Language      string    `json:"language"`
	Tracer        string    `json:"tracer"`
	TracerVersion string    `json:"tracer_version"`
	Broker        string    `json:"broker"`
	Destination   string    `json:"destination"`
	NativeID      *NativeID `json:"native_id,omitempty"`
	TraceID       string    `json:"trace_id"`
	SpanID        string    `json:"span_id"`
	TsNS          int64     `json:"ts_ns"`
}

const (
	Producer  = "producer"
	Consumer  = "consumer"
	BridgeIn  = "bridge_in"
	BridgeOut = "bridge_out"
)

// ReadDir reads every ledger/*.jsonl file of a run dir, in file name order.
func ReadDir(dir string) ([]Row, error) {
	files, err := filepath.Glob(filepath.Join(dir, "*.jsonl"))
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	var rows []Row
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			return nil, err
		}
		sc := bufio.NewScanner(bytes.NewReader(b))
		sc.Buffer(nil, 1<<20)
		for ln := 1; sc.Scan(); ln++ {
			line := bytes.TrimSpace(sc.Bytes())
			if len(line) == 0 {
				continue
			}
			var r Row
			if err := json.Unmarshal(line, &r); err != nil {
				return nil, fmt.Errorf("%s:%d: %w", f, ln, err)
			}
			switch r.Side {
			case Producer, Consumer, BridgeIn, BridgeOut:
			default:
				return nil, fmt.Errorf("%s:%d: unknown side %q", f, ln, r.Side)
			}
			rows = append(rows, r)
		}
		if err := sc.Err(); err != nil {
			return nil, fmt.Errorf("%s: %w", f, err)
		}
	}
	return rows, nil
}
