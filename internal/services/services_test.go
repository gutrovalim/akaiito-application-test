package services_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gutrovalim/akaiito-application-test/internal/services"
)

var allowlisted = map[string]string{
	"Content-Type":                 "application/x-protobuf",
	"Content-Encoding":             "gzip",
	"Datadog-Meta-Lang":            "java",
	"Datadog-Meta-Lang-Version":    "21",
	"Datadog-Meta-Tracer-Version":  "1.66.0",
	"User-Agent":                   "Datadog Trace Agent/7.83.2",
	"X-Datadog-Reported-Languages": "java",
	"Dd-Agent-Version":             "7.83.2",
}

type meta struct {
	Seq     int64             `json:"seq"`
	TsNS    int64             `json:"ts_ns"`
	Method  string            `json:"method"`
	Path    string            `json:"path"`
	Headers map[string]string `json:"headers"`
}

func readMeta(t *testing.T, path string) meta {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var m meta
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	return m
}

func TestRecorderAllowlistAndForward(t *testing.T) {
	type got struct {
		method, path string
		body         []byte
	}
	var up got
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		up = got{r.Method, r.URL.RequestURI(), b}
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte("upstream-ok"))
	}))
	defer upstream.Close()
	dir := t.TempDir()
	h, err := services.Recorder(dir, upstream.URL)
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewServer(h)
	defer rec.Close()

	body := []byte{0x1f, 0x8b, 0x00, 0xff, 'a', 0x00, 'z'}
	req, _ := http.NewRequest(http.MethodPost, rec.URL+"/api/v0.2/traces", bytes.NewReader(body))
	for k, v := range allowlisted {
		req.Header.Set(strings.ToLower(k), v)
	}
	req.Header.Set("DD-Api-Key", "secret-key-value")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	rb, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted || string(rb) != "upstream-ok" {
		t.Errorf("relayed response = %d %q, want 202 %q", resp.StatusCode, rb, "upstream-ok")
	}
	if up.method != http.MethodPost || up.path != "/api/v0.2/traces" || !bytes.Equal(up.body, body) {
		t.Errorf("upstream got %s %s %x, want POST /api/v0.2/traces %x", up.method, up.path, up.body, body)
	}

	m := readMeta(t, filepath.Join(dir, "000001.meta.json"))
	if m.Seq != 1 || m.Method != http.MethodPost || m.Path != "/api/v0.2/traces" || m.TsNS == 0 {
		t.Errorf("meta = %+v", m)
	}
	for k, v := range allowlisted {
		if m.Headers[k] != v {
			t.Errorf("meta header %s = %q, want %q", k, m.Headers[k], v)
		}
	}
	if len(m.Headers) != len(allowlisted) {
		t.Errorf("meta headers %v, want only the %d allowlisted", m.Headers, len(allowlisted))
	}
	raw, _ := os.ReadFile(filepath.Join(dir, "000001.meta.json"))
	if bytes.Contains(bytes.ToLower(raw), []byte("dd-api-key")) || bytes.Contains(raw, []byte("secret-key-value")) {
		t.Errorf("meta leaks the credential header: %s", raw)
	}
	if b, _ := os.ReadFile(filepath.Join(dir, "000001.body")); !bytes.Equal(b, body) {
		t.Errorf("stored body %x, want %x", b, body)
	}
}

func TestLedgerCollectorContract(t *testing.T) {
	dir := t.TempDir()
	h, err := services.Ledger(dir)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	defer srv.Close()
	post := func(body string) int {
		resp, err := http.Post(srv.URL+"/v1/rows", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	row := func(seq int, side, svc string) string {
		return `{"run_id":"k0-20260926T120000Z-ab12","scenario_id":"k0","seq":` + string(rune('0'+seq)) +
			`,"side":"` + side + `","service":"` + svc + `","language":"java","tracer":"dd-java","tracer_version":"1.66.0",` +
			`"broker":"kafka","destination":"orders","native_id":{"partition":0,"offset":` + string(rune('0'+seq)) + `},` +
			`"trace_id":"0000000000000000000000000000000` + string(rune('0'+seq)) + `","span_id":"000000000000000` + string(rune('0'+seq)) + `","ts_ns":1}`
	}
	if c := post("[" + row(1, "producer", "akt-producer") + "," + row(1, "consumer", "akt-consumer") + "]"); c != http.StatusNoContent {
		t.Fatalf("valid rows: status %d, want 204", c)
	}
	if c := post("[" + row(2, "producer", "akt-producer") + "]"); c != http.StatusNoContent {
		t.Fatalf("valid rows: status %d, want 204", c)
	}
	lines := func(svc string) []map[string]any {
		b, err := os.ReadFile(filepath.Join(dir, svc+".jsonl"))
		if err != nil {
			t.Fatal(err)
		}
		var out []map[string]any
		for _, l := range strings.Split(strings.TrimSpace(string(b)), "\n") {
			var m map[string]any
			if err := json.Unmarshal([]byte(l), &m); err != nil {
				t.Fatalf("%s.jsonl line %q: %v", svc, l, err)
			}
			out = append(out, m)
		}
		return out
	}
	p := lines("akt-producer")
	if len(p) != 2 || p[0]["seq"] != 1.0 || p[1]["seq"] != 2.0 || p[0]["side"] != "producer" {
		t.Errorf("akt-producer.jsonl = %v, want seqs 1, 2 appended", p)
	}
	if c := lines("akt-consumer"); len(c) != 1 || c[0]["side"] != "consumer" || c[0]["trace_id"] != "00000000000000000000000000000001" {
		t.Errorf("akt-consumer.jsonl = %v", c)
	}
	for _, bad := range []string{"not json", `{"seq":1}`, `[{"seq":"one"}]`} {
		if c := post(bad); c != http.StatusBadRequest {
			t.Errorf("body %q: status %d, want 400", bad, c)
		}
	}
}

func TestFakeIntakeStores(t *testing.T) {
	dir := t.TempDir()
	h, err := services.Intake(dir)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	defer srv.Close()
	bodies := [][]byte{[]byte("first"), {0x00, 0x01, 0x02}}
	for _, b := range bodies {
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/api/v0.2/traces", bytes.NewReader(b))
		req.Header.Set("Content-Encoding", "zstd")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("status %d, want 200", resp.StatusCode)
		}
	}
	for i, b := range bodies {
		base := filepath.Join(dir, []string{"000001", "000002"}[i])
		if got, _ := os.ReadFile(base + ".body"); !bytes.Equal(got, b) {
			t.Errorf("%s.body = %x, want %x", base, got, b)
		}
		m := readMeta(t, base+".meta.json")
		if m.Seq != int64(i+1) || m.Path != "/api/v0.2/traces" || m.Headers["Content-Encoding"] != "zstd" {
			t.Errorf("%s.meta.json = %+v", base, m)
		}
	}
}
