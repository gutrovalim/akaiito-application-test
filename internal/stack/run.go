package stack

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/gutrovalim/akaiito-application-test/internal/scenario"
)

const (
	quietFor  = 15 * time.Second
	quietCap  = 120 * time.Second
	pollEvery = 2 * time.Second
)

// NewRunID returns <scenario_id>-<UTC yyyymmddThhmmssZ>-<4 hex>.
func NewRunID(scenarioID string, now time.Time) string {
	b := make([]byte, 2)
	rand.Read(b)
	return fmt.Sprintf("%s-%s-%s", scenarioID, now.UTC().Format("20060102T150405Z"), hex.EncodeToString(b))
}

// FindRepo walks up from dir to the directory holding cmd/services/Dockerfile.
func FindRepo(dir string) (string, error) {
	d, err := filepath.Abs(dir)
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(d, "cmd", "services", "Dockerfile")); err == nil {
			return d, nil
		}
		p := filepath.Dir(d)
		if p == d {
			return "", fmt.Errorf("repo root (cmd/services/Dockerfile) not found above %s", dir)
		}
		d = p
	}
}

// Run executes one scenario run: prepares runs/<run_id>/, builds images, brings the stack up,
// waits for the apps and the agent flush, collects logs and tears the stack down.
// It returns the run dir whenever one was created; err reports a failed stack or app.
func Run(ctx context.Context, scenarioPath, runsDir, repo string, log io.Writer) (string, error) {
	raw, err := os.ReadFile(scenarioPath)
	if err != nil {
		return "", err
	}
	sc, err := scenario.Load(scenarioPath)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(runsDir, 0o755); err != nil {
		return "", err
	}
	var runID, dir string
	for {
		runID = NewRunID(sc.ID, time.Now())
		dir, err = filepath.Abs(filepath.Join(runsDir, runID))
		if err != nil {
			return "", err
		}
		if err = os.Mkdir(dir, 0o755); err == nil {
			break
		}
		if !errors.Is(err, os.ErrExist) {
			return "", err
		}
	}
	for _, sub := range []string{"ledger", "recorder", "intake", "metrics", "logs"} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o755); err != nil {
			return dir, err
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "scenario.yaml"), raw, 0o644); err != nil {
		return dir, err
	}
	if err := os.WriteFile(filepath.Join(dir, "stack.json"), Info(sc).JSON(), 0o644); err != nil {
		return dir, err
	}
	f, err := Compose(sc, runID, dir)
	if err != nil {
		return dir, err
	}
	y, err := f.YAML()
	if err != nil {
		return dir, err
	}
	composePath := filepath.Join(dir, "compose.yaml")
	if err := os.WriteFile(composePath, y, 0o644); err != nil {
		return dir, err
	}
	fmt.Fprintf(log, "run %s: dir %s\n", runID, dir)

	buildLog, err := os.Create(filepath.Join(dir, "logs", "build.log"))
	if err != nil {
		return dir, err
	}
	defer buildLog.Close()
	for _, b := range Builds {
		fmt.Fprintf(log, "run %s: building %s\n", runID, b.Image)
		c := exec.CommandContext(ctx, "docker", "build", "-t", b.Image, "-f", filepath.Join(repo, b.Dockerfile), filepath.Join(repo, b.Context))
		c.Stdout, c.Stderr = buildLog, buildLog
		if err := c.Run(); err != nil {
			return dir, fmt.Errorf("docker build %s: %w (see logs/build.log)", b.Image, err)
		}
	}

	project := Project(runID)
	compose := func(ctx context.Context, args ...string) *exec.Cmd {
		return exec.CommandContext(ctx, "docker", append([]string{"compose", "-p", project, "-f", composePath}, args...)...)
	}
	defer func() {
		c := compose(context.Background(), "down", "-v", "--remove-orphans", "--timeout", "5")
		c.Stdout, c.Stderr = buildLog, buildLog
		if err := c.Run(); err != nil {
			fmt.Fprintf(log, "run %s: compose down: %v\n", runID, err)
		}
	}()
	defer collectLogs(compose, dir, f)

	fmt.Fprintf(log, "run %s: compose up\n", runID)
	up := compose(ctx, "up", "-d", "--quiet-pull")
	up.Stdout, up.Stderr = buildLog, buildLog
	if err := up.Run(); err != nil {
		return dir, fmt.Errorf("compose up: %w (see logs/build.log)", err)
	}

	apps := Apps(sc)
	secs := 0.0
	for _, p := range sc.Phases() {
		secs += float64(p.Messages) / p.RatePerS
	}
	deadline := time.Now().Add(time.Duration(secs*float64(time.Second)) + 5*time.Minute)
	var exits map[string]int
	for {
		exits, err = exitCodes(ctx, compose, apps)
		if err != nil {
			return dir, err
		}
		if len(exits) == len(apps) {
			break
		}
		if time.Now().After(deadline) {
			return dir, fmt.Errorf("apps still running at the deadline (exited: %v)", exits)
		}
		select {
		case <-ctx.Done():
			return dir, ctx.Err()
		case <-time.After(pollEvery):
		}
	}
	fmt.Fprintf(log, "run %s: apps exited %v; waiting for the agent to flush\n", runID, exits)
	waitQuiet(ctx, filepath.Join(dir, "recorder"))

	var failed []string
	for _, a := range apps {
		if exits[a] != 0 {
			failed = append(failed, fmt.Sprintf("%s exit %d", a, exits[a]))
		}
	}
	if len(failed) > 0 {
		return dir, fmt.Errorf("apps failed: %s (see logs/)", strings.Join(failed, ", "))
	}
	return dir, nil
}

type psEntry struct {
	Service  string `json:"Service"`
	State    string `json:"State"`
	ExitCode int    `json:"ExitCode"`
}

// exitCodes returns the exit codes of the listed services whose containers have exited.
func exitCodes(ctx context.Context, compose func(context.Context, ...string) *exec.Cmd, services []string) (map[string]int, error) {
	out, err := compose(ctx, append([]string{"ps", "-a", "--format", "json"}, services...)...).Output()
	if err != nil {
		return nil, fmt.Errorf("compose ps: %w", err)
	}
	var entries []psEntry
	out = bytes.TrimSpace(out)
	if bytes.HasPrefix(out, []byte("[")) {
		err = json.Unmarshal(out, &entries)
	} else {
		sc := bufio.NewScanner(bytes.NewReader(out))
		for sc.Scan() && err == nil {
			var e psEntry
			if err = json.Unmarshal(sc.Bytes(), &e); err == nil {
				entries = append(entries, e)
			}
		}
	}
	if err != nil {
		return nil, fmt.Errorf("compose ps output: %w", err)
	}
	exits := map[string]int{}
	for _, e := range entries {
		if slices.Contains(services, e.Service) && (e.State == "exited" || e.State == "dead") {
			exits[e.Service] = e.ExitCode
		}
	}
	return exits, nil
}

// waitQuiet returns once dir has not gained files for quietFor, or after quietCap.
func waitQuiet(ctx context.Context, dir string) {
	count := func() int {
		es, _ := os.ReadDir(dir)
		return len(es)
	}
	start, last, n := time.Now(), time.Now(), count()
	for time.Since(start) < quietCap && time.Since(last) < quietFor {
		select {
		case <-ctx.Done():
			return
		case <-time.After(pollEvery):
		}
		if c := count(); c != n {
			n, last = c, time.Now()
		}
	}
}

func collectLogs(compose func(context.Context, ...string) *exec.Cmd, dir string, f *File) {
	for name := range f.Services {
		out, err := os.Create(filepath.Join(dir, "logs", name+".log"))
		if err != nil {
			continue
		}
		c := compose(context.Background(), "logs", "--no-color", "--timestamps", name)
		c.Stdout, c.Stderr = out, out
		c.Run()
		out.Close()
	}
}
