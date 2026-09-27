//go:build e2e

package e2e

import (
	"bufio"
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// repoRoot walks up from the test's working directory to the Go module root.
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("go.mod not found above %s", dir)
		}
		dir = parent
	}
}

var runIDLine = regexp.MustCompile(`^run_id=(\S+) dir=(\S+) verdict=(\S+)$`)

// runScenario runs `harness run <scenario>` into a per-test runs dir and returns the run dir.
func runScenario(t *testing.T, scenario string, runsDir string) string {
	t.Helper()
	repo := repoRoot(t)
	if runsDir == "" {
		runsDir = t.TempDir()
	}
	cmd := exec.Command("go", "run", "./cmd/harness", "run", scenario, "--runs-dir", runsDir)
	cmd.Dir = repo
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	if err != nil {
		t.Fatalf("harness run %s: %v\n%s", scenario, err, out.String())
	}
	sc := bufio.NewScanner(bytes.NewReader(out.Bytes()))
	for sc.Scan() {
		if m := runIDLine.FindStringSubmatch(strings.TrimSpace(sc.Text())); m != nil {
			if m[3] != "PASS" {
				t.Fatalf("harness run %s: verdict %s\n%s", scenario, m[3], out.String())
			}
			return m[2]
		}
	}
	t.Fatalf("harness run %s: no run_id line in output:\n%s", scenario, out.String())
	return ""
}

// report is the subset of report.json the e2e assertions read.
type report struct {
	RunID      string `json:"run_id"`
	ScenarioID string `json:"scenario_id"`
	Mode       string `json:"mode"`
	Verdict    string `json:"verdict"`
	Tiers      struct {
		O0 struct {
			Verdict   string            `json:"verdict"`
			Deduped   int               `json:"deduped"`
			Missing   []any             `json:"missing"`
			Producers map[string]string `json:"true_producers"`
		} `json:"O0"`
		O1 struct {
			Verdict string `json:"verdict"`
			Truth   struct {
				Total int `json:"total"`
			} `json:"truth"`
		} `json:"O1"`
	} `json:"tiers"`
}

func readReport(t *testing.T, runDir string) report {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(runDir, "report.json"))
	if err != nil {
		t.Fatal(err)
	}
	var r report
	if err := json.Unmarshal(b, &r); err != nil {
		t.Fatalf("report.json: %v", err)
	}
	return r
}

// row is the subset of LedgerRow the e2e assertions read.
type row struct {
	Seq             int64  `json:"seq"`
	Side            string `json:"side"`
	Service         string `json:"service"`
	Destination     string `json:"destination"`
	TraceID         string `json:"trace_id"`
	SpanID          string `json:"span_id"`
	MarkerInHeaders bool   `json:"marker_in_headers"`
	NativeID        struct {
		Partition *int   `json:"partition"`
		Offset    *int64 `json:"offset"`
		MessageID string `json:"message_id"`
	} `json:"native_id"`
}

func readLedger(t *testing.T, runDir, service string) []row {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(runDir, "ledger", service+".jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var rows []row
	for _, line := range bytes.Split(bytes.TrimSpace(b), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var r row
		if err := json.Unmarshal(line, &r); err != nil {
			t.Fatalf("ledger row: %v (%s)", err, line)
		}
		rows = append(rows, r)
	}
	return rows
}

func countFiles(t *testing.T, dir string) int {
	t.Helper()
	es, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range es {
		if strings.HasSuffix(e.Name(), ".body") {
			n++
		}
	}
	return n
}

func check(t *testing.T, cond bool, format string, args ...any) {
	t.Helper()
	if !cond {
		t.Errorf(format, args...)
	}
}
