package stack_test

import (
	"strings"
	"testing"

	"github.com/gutrovalim/akaiito-application-test/internal/scenario"
	"github.com/gutrovalim/akaiito-application-test/internal/stack"
	"gopkg.in/yaml.v3"
)

const fanOut = `id: k0-fanout
broker: kafka
topology:
  - {role: producer, lang: java, tracer: dd-java, version: "1.66.0", topic: orders}
  - {name: billing, role: consumer, lang: java, tracer: dd-java, topic: orders}
  - {name: audit, role: consumer, lang: java, topic: orders}
topics: {orders: {partitions: 3}}
traffic: {messages: 10, rate_per_s: 5}
`

func TestComposePinsImages(t *testing.T) {
	sc, err := scenario.Parse([]byte(fanOut))
	if err != nil {
		t.Fatal(err)
	}
	f, err := stack.Compose(sc, "k0-fanout-20260926T120000Z-ab12", "/tmp/runs/k0-fanout-20260926T120000Z-ab12")
	if err != nil {
		t.Fatal(err)
	}
	b, err := f.YAML()
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Services map[string]struct {
			Image string `yaml:"image"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(b, &doc); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"kafka", "agent", "recorder", "intake", "ledger", "akt-producer", "billing", "audit"} {
		if _, ok := doc.Services[want]; !ok {
			t.Errorf("compose lacks service %q", want)
		}
	}
	for name, s := range doc.Services {
		ref := s.Image
		if ref == "" {
			t.Errorf("service %s: no image", name)
			continue
		}
		repo, tag, ok := strings.Cut(ref[strings.LastIndex(ref, "/")+1:], ":")
		tag, _, _ = strings.Cut(tag, "@")
		if !ok || repo == "" || tag == "" || tag == "latest" {
			t.Errorf("service %s: image %q lacks an explicit non-latest tag", name, ref)
		}
	}
}
