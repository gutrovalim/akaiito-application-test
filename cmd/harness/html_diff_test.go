package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestReportHTMLFromJSONOnly(t *testing.T) {
	dir := writeRun(t, runID, k0Scenario, k0Rows([]int64{1, 2, 3}, []int64{1, 2, 3}))
	runReport(t, dir)
	first, err := os.ReadFile(filepath.Join(dir, "report.html"))
	must(t, err)
	jsonBefore, err := os.ReadFile(filepath.Join(dir, "report.json"))
	must(t, err)
	must(t, os.RemoveAll(filepath.Join(dir, "ledger")))
	must(t, os.Remove(filepath.Join(dir, "scenario.yaml")))
	must(t, os.Remove(filepath.Join(dir, "report.html")))
	if code, _, stderr := harness(t, "report", "--html-only", dir); code != 0 {
		t.Fatalf("report --html-only exit %d: %s", code, stderr)
	}
	second, err := os.ReadFile(filepath.Join(dir, "report.html"))
	must(t, err)
	if len(first) == 0 || !bytes.Equal(first, second) {
		t.Errorf("re-rendered report.html differs from the first render")
	}
	jsonAfter, err := os.ReadFile(filepath.Join(dir, "report.json"))
	must(t, err)
	if !bytes.Equal(jsonBefore, jsonAfter) {
		t.Errorf("--html-only changed report.json")
	}
}

func TestReportHTMLShowsTiers(t *testing.T) {
	runs := map[string][]row{
		"pass":    k0Rows([]int64{1, 2}, []int64{1, 2}),
		"invalid": k0Rows([]int64{1}, []int64{1, 2}),
	}
	cases := []struct{ run, tier, verdict string }{
		{"pass", "O0", "PASS"},
		{"pass", "O1", "REPORTED"},
		{"pass", "O1v", "REPORTED"},
		{"pass", "O2", "REPORTED"},
		{"pass", "rung2", "REPORTED"},
		{"invalid", "O0", "INVALID"},
	}
	html := map[string]string{}
	for name, rows := range runs {
		dir := writeRun(t, runID, k0Scenario, rows)
		runReport(t, dir)
		b, err := os.ReadFile(filepath.Join(dir, "report.html"))
		must(t, err)
		html[name] = string(b)
		for _, want := range []string{"k0", "7.83.2"} {
			if !strings.Contains(html[name], want) {
				t.Errorf("%s report.html lacks %q", name, want)
			}
		}
	}
	for _, tc := range cases {
		t.Run(tc.run+"/"+tc.tier, func(t *testing.T) {
			re := regexp.MustCompile(`<th>` + regexp.QuoteMeta(tc.tier) + `</th>\s*<td class="verdict[^"]*">` + tc.verdict + `</td>`)
			if !re.MatchString(html[tc.run]) {
				t.Errorf("report.html lacks tier %s with verdict %s", tc.tier, tc.verdict)
			}
		})
	}
}

func TestDiffPerTier(t *testing.T) {
	a := writeRun(t, runID, k0Scenario, k0Rows([]int64{1, 2, 3}, []int64{1, 2, 3}))
	a2 := writeRun(t, "k0-20260926T130000Z-cd34", k0Scenario, k0Rows([]int64{1, 2, 3}, []int64{1, 2, 3}))
	b := writeRun(t, "k0-20260926T140000Z-ef56", k0Scenario, k0Rows([]int64{1, 3}, []int64{1, 2, 3}))
	for _, d := range []string{a, a2, b} {
		runReport(t, d)
	}
	diff := func(x, y string) (string, map[string]any) {
		t.Helper()
		code, out, stderr := harness(t, "diff", filepath.Join(x, "report.json"), filepath.Join(y, "report.json"))
		if code != 0 {
			t.Fatalf("diff exit %d: %s", code, stderr)
		}
		var m map[string]any
		must(t, json.Unmarshal([]byte(out), &m))
		return strings.TrimSpace(out), m
	}

	t.Run("differ", func(t *testing.T) {
		_, m := diff(a, b)
		tiers := get(t, m, "tiers").(map[string]any)
		if len(tiers) != 1 {
			t.Errorf("tiers = %v, want only O0", tiers)
		}
		v, _ := json.Marshal(get(t, tiers, "O0", "verdict"))
		if string(v) != `["PASS","INVALID"]` {
			t.Errorf("tiers.O0.verdict = %s, want [\"PASS\",\"INVALID\"]", v)
		}
		changes, _ := get(t, tiers, "O0", "changes").([]any)
		found := false
		for _, c := range changes {
			c := c.(map[string]any)
			for _, k := range []string{"path", "a", "b"} {
				if _, ok := c[k]; !ok {
					t.Errorf("change %v lacks %q", c, k)
				}
			}
			if c["path"] == "missing[0].seq" && c["a"] == nil && c["b"] == 2.0 {
				found = true
			}
		}
		if !found {
			t.Errorf("changes = %v, want {path:missing[0].seq, a:null, b:2}", changes)
		}
	})

	for name, pair := range map[string][2]string{"same report": {a, a}, "same tiers, other run": {a, a2}} {
		t.Run(name, func(t *testing.T) {
			if out, _ := diff(pair[0], pair[1]); out != `{"tiers":{}}` {
				t.Errorf("diff = %s, want {\"tiers\":{}}", out)
			}
		})
	}
}
