package stack

import (
	"encoding/json"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/gutrovalim/akaiito-application-test/internal/oracle"
	"github.com/gutrovalim/akaiito-application-test/internal/scenario"
	"gopkg.in/yaml.v3"
)

const (
	DummyAPIKey   = "aktdummyapikey000000000000000000"
	AgentVersion  = "7.83.2"
	KafkaVersion  = "4.3.1"
	AgentImage    = "datadog/agent:" + AgentVersion
	KafkaImage    = "apache/kafka:" + KafkaVersion
	ServicesImage = "akt-services:local"
	JavaImage     = "akt-java:local"
	DataDir       = "/data"
	Bootstrap     = "kafka:9092"
	// RateLimit is the apps' tracer rate limit. The default of 100 traces/s is low enough
	// for a bench to trip: the ledger client's own HTTP posts are traced, so a 50 msg/s
	// scenario offers about 100 traces/s and the limiter drops whole traces. A drop at the
	// tracer propagates, so both sides of that trace disappear and the run looks like lost
	// propagation rather than a limiter artifact.
	RateLimit = "100000"
)

// DefaultVersions is the tracer version used when a topology entry names none.
var DefaultVersions = map[string]string{"dd-java": "1.66.0", "otel-java": "2.31.1"}

// Build is a locally built image: docker build -t Image -f Dockerfile Context (paths relative to the repo root).
type Build struct{ Image, Context, Dockerfile string }

var Builds = []Build{
	{ServicesImage, ".", "cmd/services/Dockerfile"},
	{JavaImage, "apps/java", "apps/java/Dockerfile"},
}

type Health struct {
	Test        []string `yaml:"test"`
	Interval    string   `yaml:"interval"`
	Timeout     string   `yaml:"timeout"`
	Retries     int      `yaml:"retries"`
	StartPeriod string   `yaml:"start_period,omitempty"`
}

type Dep struct {
	Condition string `yaml:"condition"`
}

type Service struct {
	Image       string            `yaml:"image"`
	PullPolicy  string            `yaml:"pull_policy,omitempty"`
	Entrypoint  []string          `yaml:"entrypoint,omitempty"`
	Command     []string          `yaml:"command,omitempty"`
	Environment map[string]string `yaml:"environment,omitempty"`
	Volumes     []string          `yaml:"volumes,omitempty"`
	DependsOn   map[string]Dep    `yaml:"depends_on,omitempty"`
	Healthcheck *Health           `yaml:"healthcheck,omitempty"`
}

type File struct {
	Name     string             `yaml:"name"`
	Services map[string]Service `yaml:"services"`
	Networks map[string]Network `yaml:"networks"`
}

// Network is a compose network; internal blocks egress so a run cannot reach the internet.
type Network struct {
	Internal bool `yaml:"internal"`
}

func (f *File) YAML() ([]byte, error) { return yaml.Marshal(f) }

// Project is the compose project name of a run: compose accepts lower case only.
func Project(runID string) string { return strings.ToLower(runID) }

type Tracer struct {
	Service string `json:"service"`
	Tracer  string `json:"tracer"`
	Version string `json:"version"`
}

type StackInfo struct {
	AgentVersion string   `json:"agent_version"`
	KafkaVersion string   `json:"kafka_version"`
	Tracers      []Tracer `json:"tracers"`
}

func tracerOf(e scenario.Entry) (string, string) {
	t := e.Tracer
	if t == "" {
		t = "dd-java"
	}
	v := e.Version
	if v == "" {
		v = DefaultVersions[t]
	}
	return t, v
}

// Info is the run's stack.json.
func Info(sc *scenario.Scenario) StackInfo {
	s := StackInfo{AgentVersion: AgentVersion, KafkaVersion: KafkaVersion, Tracers: []Tracer{}}
	for _, e := range sc.Topology {
		t, v := tracerOf(e)
		s.Tracers = append(s.Tracers, Tracer{e.Service, t, v})
	}
	return s
}

func (s StackInfo) JSON() []byte {
	b, _ := json.Marshal(s)
	return b
}

// Apps lists the compose services running topology entries, in topology order.
func Apps(sc *scenario.Scenario) []string {
	var out []string
	for _, e := range sc.Topology {
		out = append(out, e.Service)
	}
	return out
}

var (
	topicName   = regexp.MustCompile(`^[a-zA-Z0-9._-]{1,249}$`)
	configToken = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)
	reserved    = map[string]bool{"kafka": true, "kafka-init": true, "agent": true, "recorder": true, "intake": true, "ledger": true, "metrics": true}
)

func health(cmd string) *Health {
	return &Health{Test: []string{"CMD-SHELL", cmd}, Interval: "2s", Timeout: "10s", Retries: 90}
}

// Compose generates the compose file of one run; runDir is the absolute host path bind-mounted at /data.
func Compose(sc *scenario.Scenario, runID, runDir string) (*File, error) {
	if sc.Broker != "kafka" {
		return nil, fmt.Errorf("broker %q not supported by the stack yet", sc.Broker)
	}
	mount := []string{runDir + ":" + DataDir}
	svc := map[string]Service{
		"kafka": {
			Image: KafkaImage,
			Environment: map[string]string{
				"KAFKA_NODE_ID":                                  "1",
				"KAFKA_PROCESS_ROLES":                            "broker,controller",
				"KAFKA_LISTENERS":                                "PLAINTEXT://:9092,CONTROLLER://:9093",
				"KAFKA_ADVERTISED_LISTENERS":                     "PLAINTEXT://" + Bootstrap,
				"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
				"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
				"KAFKA_CONTROLLER_QUORUM_VOTERS":                 "1@kafka:9093",
				"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
				"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
				"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
				"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
				"KAFKA_AUTO_CREATE_TOPICS_ENABLE":                "false",
			},
			Healthcheck: health("/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 >/dev/null 2>&1"),
		},
		"agent": {
			Image: AgentImage,
			Environment: map[string]string{
				"DD_API_KEY":               DummyAPIKey,
				"DD_HOSTNAME":              "akt-agent",
				"DD_DD_URL":                "http://metrics:8080",
				"DD_APM_DD_URL":            "http://recorder:8080",
				"DD_APM_ENABLED":           "true",
				"DD_APM_NON_LOCAL_TRAFFIC": "true",
				"DD_APM_RECEIVER_PORT":     "8126",
				"DD_LOGS_ENABLED":          "false",
				"DD_PROCESS_CONFIG_PROCESS_COLLECTION_ENABLED":   "false",
				"DD_PROCESS_CONFIG_CONTAINER_COLLECTION_ENABLED": "false",
				"DD_PROCESS_CONFIG_PROCESS_DD_URL":               "http://metrics:8080",
				"DD_REMOTE_CONFIGURATION_ENABLED":                "false",
				"DD_INVENTORIES_ENABLED":                         "false",
				"DD_ENABLE_METADATA_COLLECTION":                  "false",
				"DD_CLOUD_PROVIDER_METADATA":                     "",
				"DD_DOGSTATSD_NON_LOCAL_TRAFFIC":                 "false",
				"DD_USE_DOGSTATSD":                               "false",
				"DD_SBOM_ENABLED":                                "false",
				"DD_SBOM_CONTAINER_IMAGE_ENABLED":                "false",
				"DD_SBOM_HOST_ENABLED":                           "false",
				"DD_CONTAINER_IMAGE_ENABLED":                     "false",
				"DD_CONTAINER_LIFECYCLE_ENABLED":                 "false",
				"DD_NETWORK_PATH_ENABLED":                        "false",
				"DD_SYNTHETICS_COLLECTOR_ENABLED":                "false",
				"DD_DATA_STREAMS_ENABLED":                        "false",
				"DD_ORCHESTRATOR_EXPLORER_ENABLED":               "false",
				"DD_AGENT_TELEMETRY_ENABLED":                     "false",
				"DD_APM_TELEMETRY_ENABLED":                       "false",
			},
			DependsOn:   map[string]Dep{"recorder": {"service_started"}, "metrics": {"service_started"}},
			Healthcheck: health("agent health >/dev/null 2>&1 && curl -sf http://localhost:8126/info >/dev/null"),
		},
		"recorder": {Image: ServicesImage, PullPolicy: "never", Command: []string{"recorder", "--dir", DataDir, "--upstream", "http://intake:8080"}, Volumes: mount, DependsOn: map[string]Dep{"intake": {"service_started"}}},
		"intake":   {Image: ServicesImage, PullPolicy: "never", Command: []string{"intake", "--dir", DataDir}, Volumes: mount},
		"ledger":   {Image: ServicesImage, PullPolicy: "never", Command: []string{"ledger", "--dir", DataDir}, Volumes: mount},
		"metrics":  {Image: ServicesImage, PullPolicy: "never", Command: []string{"metrics", "--dir", DataDir}, Volumes: mount},
	}

	topics := map[string]scenario.Topic{}
	maps.Copy(topics, sc.Topics)
	for _, e := range sc.Topology {
		for _, t := range []string{e.Topic, e.From, e.To} {
			if _, ok := topics[t]; t != "" && !ok {
				topics[t] = scenario.Topic{}
			}
		}
	}
	var script []string
	for _, name := range slices.Sorted(maps.Keys(topics)) {
		if !topicName.MatchString(name) {
			return nil, fmt.Errorf("topic %q: not a valid Kafka topic name", name)
		}
		t := topics[name]
		parts := max(t.Partitions, 1)
		cmd := fmt.Sprintf("/opt/kafka/bin/kafka-topics.sh --bootstrap-server %s --create --if-not-exists --topic %s --partitions %d --replication-factor 1", Bootstrap, name, parts)
		for _, k := range slices.Sorted(maps.Keys(t.Config)) {
			if !configToken.MatchString(k) || !configToken.MatchString(t.Config[k]) {
				return nil, fmt.Errorf("topics.%s.config: %s=%q not a plain config token", name, k, t.Config[k])
			}
			cmd += fmt.Sprintf(" --config %s=%s", k, t.Config[k])
		}
		script = append(script, cmd)
	}
	svc["kafka-init"] = Service{
		Image:      KafkaImage,
		Entrypoint: []string{"/bin/sh", "-ec"},
		Command:    []string{strings.Join(script, "\n")},
		DependsOn:  map[string]Dep{"kafka": {"service_healthy"}},
	}

	expect := readerExpectations(sc)
	for i, e := range sc.Topology {
		if reserved[e.Service] {
			return nil, fmt.Errorf("topology[%d]: service %q collides with a stack service", i, e.Service)
		}
		if e.Lang != "" && e.Lang != "java" {
			return nil, fmt.Errorf("topology[%d]: lang %q not supported by the stack yet", i, e.Lang)
		}
		tr, ver := tracerOf(e)
		if tr != "dd-java" {
			return nil, fmt.Errorf("topology[%d]: tracer %q not supported by the stack yet", i, tr)
		}
		if e.Role == scenario.Bridge {
			return nil, fmt.Errorf("topology[%d]: bridge role not supported by the stack yet", i)
		}
		env := map[string]string{
			"AKT_ROLE":           e.Role,
			"AKT_RUN_ID":         runID,
			"AKT_SCENARIO_ID":    sc.ID,
			"AKT_SERVICE":        e.Service,
			"AKT_BROKER":         sc.Broker,
			"AKT_BOOTSTRAP":      Bootstrap,
			"AKT_LEDGER_URL":     "http://ledger:8080",
			"AKT_IDLE_TIMEOUT_S": "60",
			"AKT_TRACER":         tr,
			"AKT_TRACER_VERSION": ver,
			"AKT_SEQ_BASE":       strconv.FormatInt(oracle.SeqBase(i), 10),
		}
		switch e.Role {
		case scenario.Producer:
			env["AKT_TOPIC"] = e.Topic
			var ph []string
			for _, p := range sc.Phases() {
				ph = append(ph, fmt.Sprintf("%d@%s", p.Messages, strconv.FormatFloat(p.RatePerS, 'f', -1, 64)))
			}
			if len(ph) == 0 {
				return nil, fmt.Errorf("topology[%d]: producer needs traffic", i)
			}
			env["AKT_PHASES"] = strings.Join(ph, ",")
		case scenario.Consumer:
			env["AKT_TOPIC"] = e.Topic
			env["AKT_EXPECT"] = strconv.Itoa(expect[e.Topic])
		}
		rate := "1.0"
		if e.Sampling != nil {
			rate = strconv.FormatFloat(*e.Sampling, 'f', -1, 64)
		}
		maps.Copy(env, map[string]string{
			"DD_SERVICE":                                  e.Service,
			"DD_AGENT_HOST":                               "agent",
			"DD_TRACE_AGENT_PORT":                         "8126",
			"DD_TRACE_OTEL_ENABLED":                       "true",
			"DD_TRACE_SAMPLE_RATE":                        rate,
			"DD_TRACE_RATE_LIMIT":                         RateLimit,
			"DD_DATA_STREAMS_ENABLED":                     "false",
			"DD_TRACE_128_BIT_TRACEID_GENERATION_ENABLED": "true",
			"DD_INSTRUMENTATION_TELEMETRY_ENABLED":        "false",
			"DD_REMOTE_CONFIGURATION_ENABLED":             "false",
		})
		maps.Copy(env, e.Env)
		svc[e.Service] = Service{
			Image:       JavaImage,
			PullPolicy:  "never",
			Environment: env,
			DependsOn: map[string]Dep{
				"kafka-init": {"service_completed_successfully"},
				"agent":      {"service_healthy"},
				"ledger":     {"service_started"},
			},
		}
	}
	return &File{Name: Project(runID), Services: svc, Networks: map[string]Network{"default": {Internal: true}}}, nil
}

// readerExpectations is the number of distinct seqs written to each destination.
func readerExpectations(sc *scenario.Scenario) map[string]int {
	out := map[string]int{}
	var written func(dest string, depth int) int
	written = func(dest string, depth int) int {
		n := 0
		for _, e := range sc.Topology {
			switch {
			case e.Role == scenario.Producer && e.Topic == dest:
				n += sc.Messages()
			case e.Role == scenario.Bridge && e.To == dest && depth < len(sc.Topology):
				n += written(e.From, depth+1)
			}
		}
		return n
	}
	for _, e := range sc.Topology {
		if d := e.Subscribes(); d != "" {
			out[d] = written(d, 0)
		}
	}
	return out
}
