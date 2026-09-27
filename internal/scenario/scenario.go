package scenario

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"strconv"

	"gopkg.in/yaml.v3"
)

type Scenario struct {
	ID       string           `yaml:"id"`
	Broker   string           `yaml:"broker"`
	Topology []Entry          `yaml:"topology"`
	Topics   map[string]Topic `yaml:"topics"`
	Traffic  Traffic          `yaml:"traffic"`
	Expect   Expect           `yaml:"expect"`
}

type Entry struct {
	Name           string            `yaml:"name"`
	Role           string            `yaml:"role"`
	Lang           string            `yaml:"lang"`
	Tracer         string            `yaml:"tracer"`
	Version        string            `yaml:"version"`
	Topic          string            `yaml:"topic"`
	From           string            `yaml:"from"`
	To             string            `yaml:"to"`
	ForwardHeaders *bool             `yaml:"forward_headers"`
	Sampling       *float64          `yaml:"sampling"`
	ClockOffset    string            `yaml:"clock_offset"`
	Env            map[string]string `yaml:"env"`
	Service        string            `yaml:"-"`
}

type Topic struct {
	Partitions int               `yaml:"partitions"`
	Config     map[string]string `yaml:"config"`
}

type Traffic struct {
	Messages int     `yaml:"messages"`
	RatePerS float64 `yaml:"rate_per_s"`
	Phases   []Phase `yaml:"phases"`
}

type Phase struct {
	Messages int     `yaml:"messages"`
	RatePerS float64 `yaml:"rate_per_s"`
}

type Expect struct {
	O0    string `yaml:"O0"`
	O1    O1     `yaml:"O1"`
	O2    *O2    `yaml:"O2"`
	Rung2 *Rung2 `yaml:"rung2"`
}

type O1 struct {
	CertainOrphans *Count `yaml:"certain_orphans"`
}

type O2 struct {
	Tolerance   float64 `yaml:"tolerance"`
	WindowMS    int64   `yaml:"window_ms"`
	LookaheadMS int64   `yaml:"lookahead_ms"`
}

type Rung2 struct {
	CandidateWindowMS int64 `yaml:"candidate_window_ms"`
}

// Count is either "from_truth" (FromTruth) or an explicit value.
type Count struct {
	FromTruth bool
	Value     int
}

func (c *Count) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind == yaml.ScalarNode {
		if n.Value == "from_truth" {
			*c = Count{FromTruth: true}
			return nil
		}
		if v, err := strconv.Atoi(n.Value); err == nil && v >= 0 {
			*c = Count{Value: v}
			return nil
		}
	}
	return fmt.Errorf("line %d: certain_orphans must be from_truth or a non-negative integer, got %q", n.Line, n.Value)
}

const (
	Producer = "producer"
	Consumer = "consumer"
	Bridge   = "bridge"
)

func Load(path string) (*Scenario, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	s, err := Parse(b)
	if err != nil {
		return nil, fmt.Errorf("invalid scenario %s: %w", path, err)
	}
	return s, nil
}

func Parse(b []byte) (*Scenario, error) {
	d := yaml.NewDecoder(bytes.NewReader(b))
	d.KnownFields(true)
	var s Scenario
	if err := d.Decode(&s); err != nil {
		return nil, err
	}
	if err := s.validate(); err != nil {
		return nil, err
	}
	s.assignServices()
	seen := map[string]int{}
	for i, e := range s.Topology {
		if j, ok := seen[e.Service]; ok {
			return nil, fmt.Errorf("topology[%d] and topology[%d] share service %q", j, i, e.Service)
		}
		seen[e.Service] = i
	}
	return &s, nil
}

func (s *Scenario) validate() error {
	var errs []error
	add := func(f string, a ...any) { errs = append(errs, fmt.Errorf(f, a...)) }
	if s.ID == "" {
		add("missing id")
	}
	switch s.Broker {
	case "kafka", "sqs", "sns":
	case "":
		add("missing broker")
	default:
		add("unknown broker %q (want kafka, sqs or sns)", s.Broker)
	}
	if len(s.Topology) == 0 {
		add("missing topology")
	}
	for i, e := range s.Topology {
		switch e.Role {
		case Producer, Consumer:
			if e.Topic == "" {
				add("topology[%d] (%s): missing topic", i, e.Role)
			}
		case Bridge:
			if e.From == "" || e.To == "" {
				add("topology[%d] (bridge): missing from/to", i)
			}
		case "":
			add("topology[%d]: missing role", i)
		default:
			add("topology[%d]: unknown role %q (want producer, consumer or bridge)", i, e.Role)
		}
		switch e.Lang {
		case "", "java", "go":
		default:
			add("topology[%d]: unknown lang %q (want java or go)", i, e.Lang)
		}
		if e.Sampling != nil && (*e.Sampling < 0 || *e.Sampling > 1) {
			add("topology[%d]: sampling %v outside [0,1]", i, *e.Sampling)
		}
	}
	for name, t := range s.Topics {
		if t.Partitions < 0 {
			add("topics.%s: negative partitions", name)
		}
	}
	t := s.Traffic
	if t.Messages < 0 || t.RatePerS < 0 {
		add("traffic: negative messages or rate_per_s")
	}
	if len(t.Phases) > 0 && (t.Messages != 0 || t.RatePerS != 0) {
		add("traffic: phases exclude messages/rate_per_s")
	}
	for i, p := range t.Phases {
		if p.Messages <= 0 || p.RatePerS <= 0 {
			add("traffic.phases[%d]: messages and rate_per_s must be positive", i)
		}
	}
	switch s.Expect.O0 {
	case "", "PASS", "INVALID":
	default:
		add("expect.O0: unknown verdict %q (want PASS or INVALID)", s.Expect.O0)
	}
	if o := s.Expect.O2; o != nil {
		if o.WindowMS < 0 {
			add("expect.O2.window_ms: negative (%d)", o.WindowMS)
		}
		if o.LookaheadMS < 0 {
			add("expect.O2.lookahead_ms: negative (%d)", o.LookaheadMS)
		}
		if o.Tolerance < 0 {
			add("expect.O2.tolerance: negative")
		}
	}
	if r := s.Expect.Rung2; r != nil && r.CandidateWindowMS < 0 {
		add("expect.rung2.candidate_window_ms: negative (%d)", r.CandidateWindowMS)
	}
	return errors.Join(errs...)
}

func (s *Scenario) assignServices() {
	n := map[string]int{}
	base := func(e Entry) string {
		if e.Name != "" {
			return e.Name
		}
		return "akt-" + e.Role
	}
	for _, e := range s.Topology {
		if e.Name == "" {
			n[base(e)]++
		}
	}
	for i := range s.Topology {
		e := &s.Topology[i]
		e.Service = base(*e)
		if e.Name == "" && n[e.Service] > 1 {
			e.Service = fmt.Sprintf("%s-%d", e.Service, i)
		}
	}
}

// Subscribes reports the destination an entry reads from, if any.
func (e Entry) Subscribes() string {
	switch e.Role {
	case Consumer:
		return e.Topic
	case Bridge:
		return e.From
	}
	return ""
}

func (e Entry) ReadSide() string {
	if e.Role == Bridge {
		return "bridge_in"
	}
	return "consumer"
}
