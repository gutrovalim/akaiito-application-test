package report

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/gutrovalim/akaiito-application-test/internal/captures"
	"github.com/gutrovalim/akaiito-application-test/internal/ledger"
	"github.com/gutrovalim/akaiito-application-test/internal/oracle"
	"github.com/gutrovalim/akaiito-application-test/internal/scenario"
)

type Tier struct {
	Verdict string         `json:"verdict"`
	Truth   map[string]any `json:"truth"`
}

type Tiers struct {
	O0    oracle.O0 `json:"O0"`
	O1    Tier      `json:"O1"`
	O1v   Tier      `json:"O1v"`
	O2    Tier      `json:"O2"`
	Rung2 Tier      `json:"rung2"`
}

type Report struct {
	Schema     int             `json:"schema"`
	RunID      string          `json:"run_id"`
	ScenarioID string          `json:"scenario_id"`
	Mode       string          `json:"mode"`
	Stack      json.RawMessage `json:"stack"`
	Verdict    string          `json:"verdict"`
	Tiers      Tiers           `json:"tiers"`
	Facts      map[string]any  `json:"facts"`
}

func reported() Tier { return Tier{Verdict: oracle.Reported, Truth: map[string]any{}} }

// Build computes the baseline report of a run dir.
func Build(dir string) (*Report, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	sc, err := scenario.Load(filepath.Join(abs, "scenario.yaml"))
	if err != nil {
		return nil, err
	}
	stack := json.RawMessage("{}")
	if b, err := os.ReadFile(filepath.Join(abs, "stack.json")); err == nil {
		var c bytes.Buffer
		if err := json.Compact(&c, b); err != nil {
			return nil, errors.New("stack.json: " + err.Error())
		}
		stack = c.Bytes()
	} else if !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	if _, err := os.Stat(filepath.Join(abs, "ledger")); err != nil {
		return nil, err
	}
	rows, err := ledger.ReadDir(filepath.Join(abs, "ledger"))
	if err != nil {
		return nil, err
	}
	caps, err := captures.ReadTraces(filepath.Join(abs, "recorder"))
	if err != nil {
		return nil, err
	}
	o0, kept := oracle.Validity(sc, rows)
	o1, o1v := oracle.Truth(sc, o0, kept, caps)
	verdict := oracle.Pass
	if o0.Verdict == oracle.Invalid {
		verdict = oracle.Invalid
	}
	return &Report{
		Schema:     1,
		RunID:      filepath.Base(abs),
		ScenarioID: sc.ID,
		Mode:       "baseline",
		Stack:      stack,
		Verdict:    verdict,
		Tiers: Tiers{
			O0:    o0,
			O1:    Tier{Verdict: oracle.Reported, Truth: oracle.AsMap(o1)},
			O1v:   Tier{Verdict: oracle.Reported, Truth: oracle.AsMap(o1v)},
			O2:    reported(),
			Rung2: reported(),
		},
		Facts: map[string]any{},
	}, nil
}

// Write builds report.json for a run dir, then renders report.html from it.
func Write(dir string) (*Report, error) {
	r, err := Build(dir)
	if err != nil {
		return nil, err
	}
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(dir, "report.json"), append(b, '\n'), 0o644); err != nil {
		return nil, err
	}
	return r, WriteHTML(dir)
}

// WriteHTML renders report.html from the run dir's report.json alone.
func WriteHTML(dir string) error {
	b, err := os.ReadFile(filepath.Join(dir, "report.json"))
	if err != nil {
		return err
	}
	h, err := RenderHTML(b)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "report.html"), h, 0o644)
}
