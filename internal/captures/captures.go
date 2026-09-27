// Package captures reads a run's recorder captures back into spans.
package captures

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/gutrovalim/akaiito-application-test/internal/services"
	"github.com/gutrovalim/akaiito-application-test/internal/trace"
)

// Capture is one recorder request: its meta, and its spans when it carried a trace payload.
type Capture struct {
	Meta  services.Meta
	Body  string
	Spans []trace.Span
	Err   error
}

// ReadTraces reads every /api/v0.2/traces capture under dir, in file-name order.
// Captures that are not trace payloads are returned with no spans.
func ReadTraces(dir string) ([]Capture, error) {
	metas, err := filepath.Glob(filepath.Join(dir, "*.meta.json"))
	if err != nil {
		return nil, err
	}
	sort.Strings(metas)
	out := make([]Capture, 0, len(metas))
	for _, mp := range metas {
		mb, err := os.ReadFile(mp)
		if err != nil {
			return nil, err
		}
		var meta services.Meta
		if err := json.Unmarshal(mb, &meta); err != nil {
			return nil, fmt.Errorf("%s: %w", mp, err)
		}
		c := Capture{Meta: meta, Body: mp[:len(mp)-len(".meta.json")] + ".body"}
		if meta.Path != "/api/v0.2/traces" {
			out = append(out, c)
			continue
		}
		raw, err := os.ReadFile(c.Body)
		if err != nil {
			return nil, err
		}
		if len(raw) == 0 {
			out = append(out, c)
			continue
		}
		c.Spans, c.Err = trace.Decode(raw, header(meta.Headers, "Content-Encoding"))
		out = append(out, c)
	}
	return out, nil
}

func header(h map[string]string, key string) string {
	for k, v := range h {
		if strings.EqualFold(k, key) {
			return v
		}
	}
	return ""
}
