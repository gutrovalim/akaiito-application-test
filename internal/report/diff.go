package report

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"
)

type Change struct {
	Path string `json:"path"`
	A    any    `json:"a"`
	B    any    `json:"b"`
}

type TierDiff struct {
	Verdict []any    `json:"verdict,omitempty"`
	Changes []Change `json:"changes,omitempty"`
}

type DiffResult struct {
	Tiers map[string]TierDiff `json:"tiers"`
}

// Diff compares the tiers of two report.json documents.
func Diff(a, b []byte) (DiffResult, error) {
	var ra, rb struct {
		Tiers map[string]map[string]any `json:"tiers"`
	}
	if err := json.Unmarshal(a, &ra); err != nil {
		return DiffResult{}, fmt.Errorf("a: %w", err)
	}
	if err := json.Unmarshal(b, &rb); err != nil {
		return DiffResult{}, fmt.Errorf("b: %w", err)
	}
	out := DiffResult{Tiers: map[string]TierDiff{}}
	names := map[string]bool{}
	for k := range ra.Tiers {
		names[k] = true
	}
	for k := range rb.Tiers {
		names[k] = true
	}
	for name := range names {
		ta, tb := ra.Tiers[name], rb.Tiers[name]
		var d TierDiff
		if va, vb := ta["verdict"], tb["verdict"]; va != vb {
			d.Verdict = []any{va, vb}
		}
		la, lb := map[string]any{}, map[string]any{}
		for k, v := range ta {
			if k != "verdict" {
				flatten(k, v, la)
			}
		}
		for k, v := range tb {
			if k != "verdict" {
				flatten(k, v, lb)
			}
		}
		paths := map[string]bool{}
		for p := range la {
			paths[p] = true
		}
		for p := range lb {
			paths[p] = true
		}
		for p := range paths {
			if !reflect.DeepEqual(la[p], lb[p]) {
				d.Changes = append(d.Changes, Change{Path: p, A: la[p], B: lb[p]})
			}
		}
		slices.SortFunc(d.Changes, func(x, y Change) int { return strings.Compare(x.Path, y.Path) })
		if d.Verdict != nil || d.Changes != nil {
			out.Tiers[name] = d
		}
	}
	return out, nil
}

func flatten(path string, v any, out map[string]any) {
	switch x := v.(type) {
	case map[string]any:
		for k, c := range x {
			flatten(path+"."+k, c, out)
		}
	case []any:
		for i, c := range x {
			flatten(fmt.Sprintf("%s[%d]", path, i), c, out)
		}
	default:
		out[path] = x
	}
}
