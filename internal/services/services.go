package services

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gutrovalim/akaiito-application-test/internal/ledger"
)

var Allowlist = []string{
	"Content-Type", "Content-Encoding", "Datadog-Meta-Lang", "Datadog-Meta-Lang-Version",
	"Datadog-Meta-Tracer-Version", "User-Agent", "X-Datadog-Reported-Languages", "Dd-Agent-Version",
}

type Meta struct {
	Seq     int64             `json:"seq"`
	TsNS    int64             `json:"ts_ns"`
	Method  string            `json:"method"`
	Path    string            `json:"path"`
	Headers map[string]string `json:"headers"`
}

// Store writes numbered captures <NNNNNN>.body and <NNNNNN>.meta.json into a directory.
type Store struct {
	dir string
	n   atomic.Int64
}

func NewStore(dir string) (*Store, error) {
	return &Store{dir: dir}, os.MkdirAll(dir, 0o755)
}

func (s *Store) Save(r *http.Request, body []byte) error {
	m := Meta{Seq: s.n.Add(1), TsNS: time.Now().UnixNano(), Method: r.Method, Path: r.URL.RequestURI(), Headers: map[string]string{}}
	for _, h := range Allowlist {
		if v := r.Header.Values(h); len(v) > 0 {
			m.Headers[h] = strings.Join(v, ", ")
		}
	}
	base := filepath.Join(s.dir, fmt.Sprintf("%06d", m.Seq))
	if err := os.WriteFile(base+".body", body, 0o644); err != nil {
		return err
	}
	b, _ := json.Marshal(m)
	return os.WriteFile(base+".meta.json", b, 0o644)
}

func capture(s *Store, w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	body, err := io.ReadAll(r.Body)
	if err == nil {
		err = s.Save(r, body)
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return nil, false
	}
	return body, true
}

// Recorder stores every request, then forwards it unchanged to upstream and relays the response.
func Recorder(dir, upstream string) (http.Handler, error) {
	s, err := NewStore(dir)
	if err != nil {
		return nil, err
	}
	up := strings.TrimRight(upstream, "/")
	client := &http.Client{Timeout: 60 * time.Second}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, ok := capture(s, w, r)
		if !ok {
			return
		}
		req, err := http.NewRequestWithContext(r.Context(), r.Method, up+r.URL.RequestURI(), bytes.NewReader(body))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		req.Header = r.Header.Clone()
		req.Host = r.Host
		resp, err := client.Do(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		for k, v := range resp.Header {
			w.Header()[k] = v
		}
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
	}), nil
}

// Intake stores every request and answers 200.
func Intake(dir string) (http.Handler, error) {
	s, err := NewStore(dir)
	if err != nil {
		return nil, err
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := capture(s, w, r); ok {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte("{}"))
		}
	}), nil
}

// Metrics stores every request; POSTs get 202, anything else 200 with an empty JSON object.
func Metrics(dir string) (http.Handler, error) {
	s, err := NewStore(dir)
	if err != nil {
		return nil, err
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := capture(s, w, r); !ok {
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusAccepted)
		}
		w.Write([]byte(`{"valid":true}`))
	}), nil
}

// Ledger accepts POST /v1/rows (JSON array of ledger rows) and appends them to <dir>/<service>.jsonl.
func Ledger(dir string) (http.Handler, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	var mu sync.Mutex
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/rows", func(w http.ResponseWriter, r *http.Request) {
		var rows []ledger.Row
		if err := json.NewDecoder(r.Body).Decode(&rows); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		by := map[string]*bytes.Buffer{}
		for _, row := range rows {
			if row.Service == "" || strings.ContainsAny(row.Service, `/\`) || strings.HasPrefix(row.Service, ".") {
				http.Error(w, fmt.Sprintf("invalid service %q", row.Service), http.StatusBadRequest)
				return
			}
			if by[row.Service] == nil {
				by[row.Service] = &bytes.Buffer{}
			}
			b, _ := json.Marshal(row)
			by[row.Service].Write(append(b, '\n'))
		}
		mu.Lock()
		defer mu.Unlock()
		for svc, b := range by {
			f, err := os.OpenFile(filepath.Join(dir, svc+".jsonl"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
			if err == nil {
				_, err = f.Write(b.Bytes())
				if cerr := f.Close(); err == nil {
					err = cerr
				}
			}
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
		w.WriteHeader(http.StatusNoContent)
	})
	return mux, nil
}
