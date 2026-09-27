//go:build e2e

package e2e

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

var runIDShape = regexp.MustCompile(`^k0-\d{8}T\d{6}Z-[0-9a-f]{4}$`)

// TestK0 is the M0 control: healthy dd-java propagation over Kafka end to end.
func TestK0(t *testing.T) {
	runsDir := t.TempDir()
	first := runScenario(t, "scenarios/k0.yaml", runsDir)
	second := runScenario(t, "scenarios/k0.yaml", runsDir)

	// C13: each run gets a unique run_id and its own artifact directory.
	for _, dir := range []string{first, second} {
		check(t, runIDShape.MatchString(filepath.Base(dir)), "run id %q does not match the run_id shape", filepath.Base(dir))
		check(t, filepath.Dir(dir) == runsDir, "run dir %q is not under the runs dir %q", dir, runsDir)
		for _, sub := range []string{"ledger", "recorder", "intake"} {
			check(t, isDir(filepath.Join(dir, sub)), "run dir has no %s/", sub)
		}
	}
	check(t, first != second, "two runs got the same directory %q", first)

	r := readReport(t, first)
	check(t, r.RunID == filepath.Base(first), "report run_id %q != dir name %q", r.RunID, filepath.Base(first))
	check(t, r.ScenarioID == "k0", "report scenario_id = %q, want k0", r.ScenarioID)

	// C14: O0 PASS with non-empty recorder and intake captures.
	check(t, r.Tiers.O0.Verdict == "PASS", "O0 verdict = %q, want PASS (missing %v)", r.Tiers.O0.Verdict, r.Tiers.O0.Missing)
	check(t, r.Verdict == "PASS", "report verdict = %q, want PASS", r.Verdict)
	check(t, countFiles(t, filepath.Join(first, "recorder")) > 0, "no recorder captures")
	check(t, countFiles(t, filepath.Join(first, "intake")) > 0, "no intake captures")

	// C15: the body marker is never carried in headers, and every consumed seq came from the producer.
	producer := readLedger(t, first, "akt-producer")
	consumer := readLedger(t, first, "akt-consumer")
	check(t, len(producer) == 200, "producer rows = %d, want 200", len(producer))
	check(t, len(consumer) == 200, "consumer rows = %d, want 200", len(consumer))
	produced := map[int64]row{}
	for _, p := range producer {
		produced[p.Seq] = p
	}
	for _, c := range consumer {
		check(t, !c.MarkerInHeaders, "consumer row seq %d has marker_in_headers=true", c.Seq)
		p, ok := produced[c.Seq]
		if !ok {
			t.Errorf("consumer row seq %d has no producer row", c.Seq)
			continue
		}
		check(t, p.Destination == c.Destination, "seq %d: producer destination %q != consumer %q", c.Seq, p.Destination, c.Destination)
	}

	// C16: the producer native_id is (partition, offset) from RecordMetadata.
	seen := map[int]map[int64]bool{}
	for _, p := range producer {
		if p.NativeID.Partition == nil || p.NativeID.Offset == nil {
			t.Fatalf("producer row seq %d has no (partition, offset): %+v", p.Seq, p.NativeID)
		}
		if seen[*p.NativeID.Partition] == nil {
			seen[*p.NativeID.Partition] = map[int64]bool{}
		}
		if seen[*p.NativeID.Partition][*p.NativeID.Offset] {
			t.Errorf("partition %d offset %d appears twice", *p.NativeID.Partition, *p.NativeID.Offset)
		}
		seen[*p.NativeID.Partition][*p.NativeID.Offset] = true
	}
}

func isDir(path string) bool {
	fi, err := os.Stat(path)
	return err == nil && fi.IsDir()
}
