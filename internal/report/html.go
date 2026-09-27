package report

import (
	"bytes"
	"encoding/json"
	"html/template"
	"slices"
	"strings"
)

var tierOrder = []string{"O0", "O1", "O1v", "O2", "rung2"}

var page = template.Must(template.New("report").Parse(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>{{.ScenarioID}} - {{.RunID}}</title>
<style>
body{font-family:sans-serif;margin:2em}table{border-collapse:collapse;margin-bottom:1.5em}
th,td{border:1px solid #ccc;padding:.3em .6em;text-align:left;vertical-align:top}
pre{margin:0;font-size:.85em}
.PASS{color:#070}.FAIL,.INVALID{color:#b00}.REPORTED{color:#555}
</style>
</head>
<body>
<h1>{{.ScenarioID}}</h1>
<table>
<tr><th>scenario</th><td id="scenario-id">{{.ScenarioID}}</td></tr>
<tr><th>run</th><td id="run-id">{{.RunID}}</td></tr>
<tr><th>mode</th><td>{{.Mode}}</td></tr>
<tr><th>verdict</th><td id="verdict" class="{{.Verdict}}">{{.Verdict}}</td></tr>
</table>
<h2>Stack</h2>
<table>
{{range .Stack}}<tr><th>{{.Key}}</th><td>{{.Value}}</td></tr>
{{end}}</table>
<h2>Tiers</h2>
<table>
<tr><th>tier</th><th>verdict</th><th>details</th></tr>
{{range .Tiers}}<tr data-tier="{{.Key}}"><th>{{.Key}}</th><td class="verdict {{.Verdict}}">{{.Verdict}}</td><td><pre>{{.Value}}</pre></td></tr>
{{end}}</table>
<h2>Facts</h2>
<pre>{{.Facts}}</pre>
</body>
</html>
`))

type kv struct{ Key, Verdict, Value string }

func pretty(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	b, _ := json.MarshalIndent(v, "", "  ")
	return string(b)
}

func sortedKeys[V any](m map[string]V, first []string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	rank := func(k string) int {
		if i := slices.Index(first, k); i >= 0 {
			return i
		}
		return len(first)
	}
	slices.SortFunc(keys, func(a, b string) int {
		if d := rank(a) - rank(b); d != 0 {
			return d
		}
		return strings.Compare(a, b)
	})
	return keys
}

// RenderHTML renders report.html from report.json bytes.
func RenderHTML(reportJSON []byte) ([]byte, error) {
	var r struct {
		RunID      string                    `json:"run_id"`
		ScenarioID string                    `json:"scenario_id"`
		Mode       string                    `json:"mode"`
		Verdict    string                    `json:"verdict"`
		Stack      map[string]any            `json:"stack"`
		Tiers      map[string]map[string]any `json:"tiers"`
		Facts      map[string]any            `json:"facts"`
	}
	if err := json.Unmarshal(reportJSON, &r); err != nil {
		return nil, err
	}
	data := struct {
		RunID, ScenarioID, Mode, Verdict, Facts string
		Stack, Tiers                            []kv
	}{RunID: r.RunID, ScenarioID: r.ScenarioID, Mode: r.Mode, Verdict: r.Verdict, Facts: pretty(r.Facts)}
	for _, k := range sortedKeys(r.Stack, nil) {
		data.Stack = append(data.Stack, kv{Key: k, Value: pretty(r.Stack[k])})
	}
	for _, k := range sortedKeys(r.Tiers, tierOrder) {
		t := r.Tiers[k]
		v, _ := t["verdict"].(string)
		rest := map[string]any{}
		for f, x := range t {
			if f != "verdict" {
				rest[f] = x
			}
		}
		data.Tiers = append(data.Tiers, kv{Key: k, Verdict: v, Value: pretty(rest)})
	}
	var buf bytes.Buffer
	if err := page.Execute(&buf, data); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
