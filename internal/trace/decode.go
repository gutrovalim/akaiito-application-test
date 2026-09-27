// Package trace decodes the Datadog agent's trace payloads into flat spans.
package trace

import (
	"bytes"
	"compress/gzip"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/DataDog/datadog-agent/pkg/proto/pbgo/trace"
	"github.com/DataDog/datadog-agent/pkg/proto/pbgo/trace/idx"
	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/proto"
)

// ZeroID is the all-zero span id: a span with this parent is a trace root.
const ZeroID = "0000000000000000"

// Span is one span of an AgentPayload, with ids already normalised to 128/64-bit hex.
type Span struct {
	TraceID  string
	SpanID   string
	ParentID string
	Service  string
	Name     string
	Resource string
	Type     string
	Meta     map[string]string
	Metrics  map[string]float64
	Start    int64
	Duration int64
}

// IsRoot reports whether the span is a trace root.
func (s Span) IsRoot() bool { return s.ParentID == ZeroID }

// String returns the Meta tag, falling back to the numeric Metrics value.
func (s Span) String(key string) (string, bool) {
	if v, ok := s.Meta[key]; ok {
		return v, true
	}
	if v, ok := s.Metrics[key]; ok {
		return formatFloat(v), true
	}
	return "", false
}

// Int returns the tag as an int64 when it holds one.
func (s Span) Int(key string) (int64, bool) {
	v, ok := s.Metrics[key]
	if !ok || v != float64(int64(v)) {
		return 0, false
	}
	return int64(v), true
}

// Kind is the span kind (Meta "span.kind"), or the idx kind name.
func (s Span) Kind() string {
	if v, ok := s.Meta["span.kind"]; ok {
		return v
	}
	return s.Meta["_idx.kind"]
}

func formatFloat(v float64) string {
	if v == float64(int64(v)) {
		return fmt.Sprintf("%d", int64(v))
	}
	return fmt.Sprintf("%g", v)
}

// Decode inflates and unmarshals an AgentPayload body, returning every span of both the
// plain and the indexed tracer-payload variants.
func Decode(body []byte, contentEncoding string) ([]Span, error) {
	raw, err := inflate(body, contentEncoding)
	if err != nil {
		return nil, err
	}
	var p trace.AgentPayload
	if err := proto.Unmarshal(raw, &p); err != nil {
		return nil, fmt.Errorf("agent payload: %w", err)
	}
	var out []Span
	for _, tp := range p.TracerPayloads {
		for _, c := range tp.Chunks {
			for _, s := range c.Spans {
				meta := map[string]string{}
				for k, v := range s.Meta {
					meta[k] = v
				}
				metrics := map[string]float64{}
				for k, v := range s.Metrics {
					metrics[k] = v
				}
				traceID := fmt.Sprintf("%016x", s.TraceID)
				if hi := meta["_dd.p.tid"]; hi != "" && len(hi) == 16 {
					traceID = strings.ToLower(hi) + traceID
				}
				out = append(out, Span{
					TraceID:  traceID,
					SpanID:   fmt.Sprintf("%016x", s.SpanID),
					ParentID: fmt.Sprintf("%016x", s.ParentID),
					Service:  s.Service,
					Name:     s.Name,
					Resource: s.Resource,
					Type:     s.Type,
					Meta:     meta,
					Metrics:  metrics,
					Start:    s.Start,
					Duration: s.Duration,
				})
			}
		}
	}
	for _, tp := range p.IdxTracerPayloads {
		out = append(out, idxSpans(tp)...)
	}
	return out, nil
}

// idxSpans flattens one indexed tracer payload, resolving its string table.
func idxSpans(tp *idx.TracerPayload) []Span {
	str := func(ref uint32) string {
		if int(ref) < len(tp.Strings) {
			return tp.Strings[ref]
		}
		return ""
	}
	var out []Span
	for _, c := range tp.Chunks {
		traceID := hex.EncodeToString(c.TraceID)
		chunkAttrs := attributes(c.Attributes, str)
		if hi, ok := chunkAttrs["_dd.p.tid"]; ok && len(traceID) == 16 && len(hi) == 16 {
			traceID = strings.ToLower(hi) + traceID
		}
		for _, s := range c.Spans {
			meta := attributes(s.Attributes, str)
			metrics := map[string]float64{}
			for k, v := range meta {
				if f, ok := numeric(v); ok {
					metrics[k] = f
					delete(meta, k)
				}
			}
			if s.Kind != 0 {
				meta["_idx.kind"] = s.Kind.String()
			}
			out = append(out, Span{
				TraceID:  traceID,
				SpanID:   fmt.Sprintf("%016x", s.SpanID),
				ParentID: fmt.Sprintf("%016x", s.ParentID),
				Service:  str(s.ServiceRef),
				Name:     str(s.NameRef),
				Resource: str(s.ResourceRef),
				Meta:     meta,
				Metrics:  metrics,
				Start:    int64(s.Start),
				Duration: int64(s.Duration),
			})
		}
	}
	return out
}

func attributes(attrs map[uint32]*idx.AnyValue, str func(uint32) string) map[string]string {
	out := map[string]string{}
	for k, v := range attrs {
		if v == nil || v.Value == nil {
			continue
		}
		switch av := v.Value.(type) {
		case *idx.AnyValue_StringValueRef:
			out[str(k)] = str(av.StringValueRef)
		case *idx.AnyValue_IntValue:
			out[str(k)] = fmt.Sprintf("%d", av.IntValue)
		case *idx.AnyValue_DoubleValue:
			out[str(k)] = formatFloat(av.DoubleValue)
		case *idx.AnyValue_BoolValue:
			out[str(k)] = fmt.Sprintf("%t", av.BoolValue)
		case *idx.AnyValue_BytesValue:
			out[str(k)] = hex.EncodeToString(av.BytesValue)
		}
	}
	return out
}

func numeric(v string) (float64, bool) {
	var f float64
	if _, err := fmt.Sscanf(v, "%g", &f); err != nil {
		return 0, false
	}
	if formatFloat(f) != v {
		return 0, false
	}
	return f, true
}

// inflate applies the payload's Content-Encoding.
func inflate(body []byte, contentEncoding string) ([]byte, error) {
	switch strings.ToLower(strings.TrimSpace(contentEncoding)) {
	case "", "identity":
		return body, nil
	case "gzip":
		z, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("gzip: %w", err)
		}
		defer z.Close()
		var b bytes.Buffer
		if _, err := b.ReadFrom(z); err != nil {
			return nil, fmt.Errorf("gzip: %w", err)
		}
		return b.Bytes(), nil
	case "zstd":
		d, err := zstd.NewReader(nil)
		if err != nil {
			return nil, fmt.Errorf("zstd: %w", err)
		}
		defer d.Close()
		out, err := d.DecodeAll(body, nil)
		if err != nil {
			return nil, fmt.Errorf("zstd: %w", err)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported Content-Encoding %q", contentEncoding)
	}
}
