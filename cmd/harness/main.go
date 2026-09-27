package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"

	"github.com/gutrovalim/akaiito-application-test/internal/report"
	"github.com/gutrovalim/akaiito-application-test/internal/stack"
)

const usage = `usage:
  harness report [--html-only] <run_dir>
  harness diff <a/report.json> <b/report.json>
  harness run <scenario.yaml> [--runs-dir runs]
  harness record <scenario.yaml> [--fixtures-dir fixtures]
`

func main() { os.Exit(Main(os.Args[1:], os.Stdout, os.Stderr)) }

// Main is the CLI entry: 0 success, 1 failure, 2 usage error.
func Main(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	fail := func(err error) int {
		fmt.Fprintf(stderr, "harness %s: %v\n", args[0], err)
		return 1
	}
	switch cmd, rest := args[0], args[1:]; cmd {
	case "report":
		htmlOnly := len(rest) == 2 && rest[0] == "--html-only"
		if htmlOnly {
			rest = rest[1:]
		}
		if len(rest) != 1 {
			fmt.Fprint(stderr, usage)
			return 2
		}
		if htmlOnly {
			if err := report.WriteHTML(rest[0]); err != nil {
				return fail(err)
			}
			return 0
		}
		r, err := report.Write(rest[0])
		if err != nil {
			return fail(err)
		}
		fmt.Fprintf(stdout, "run_id=%s dir=%s verdict=%s\n", r.RunID, rest[0], r.Verdict)
		return 0
	case "diff":
		if len(rest) != 2 {
			fmt.Fprint(stderr, usage)
			return 2
		}
		a, err := os.ReadFile(rest[0])
		if err != nil {
			return fail(err)
		}
		b, err := os.ReadFile(rest[1])
		if err != nil {
			return fail(err)
		}
		d, err := report.Diff(a, b)
		if err != nil {
			return fail(err)
		}
		out, _ := json.Marshal(d)
		fmt.Fprintln(stdout, string(out))
		return 0
	case "run":
		if len(rest) == 3 && rest[1] == "--runs-dir" {
			rest = []string{rest[0], rest[2]}
		} else if len(rest) == 1 {
			rest = append(rest, "runs")
		} else {
			fmt.Fprint(stderr, usage)
			return 2
		}
		return run(rest[0], rest[1], stdout, stderr)
	case "record":
		return fail(fmt.Errorf("not implemented"))
	default:
		fmt.Fprint(stderr, usage)
		return 2
	}
}

// run executes a scenario, then writes its report through the same path as `harness report`.
func run(scenarioPath, runsDir string, stdout, stderr io.Writer) int {
	repo, err := stack.FindRepo(".")
	if err != nil {
		if repo, err = stack.FindRepo(filepath.Dir(scenarioPath)); err != nil {
			fmt.Fprintf(stderr, "harness run: %v\n", err)
			return 1
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	dir, runErr := stack.Run(ctx, scenarioPath, runsDir, repo, stderr)
	if dir == "" {
		fmt.Fprintf(stderr, "harness run: %v\n", runErr)
		return 1
	}
	code := 0
	if runErr != nil {
		fmt.Fprintf(stderr, "harness run: %v\n", runErr)
		code = 1
	}
	r, err := report.Write(dir)
	if err != nil {
		fmt.Fprintf(stderr, "harness run: report: %v\n", err)
		fmt.Fprintf(stdout, "run_id=%s dir=%s verdict=NONE\n", filepath.Base(dir), dir)
		return 1
	}
	fmt.Fprintf(stdout, "run_id=%s dir=%s verdict=%s\n", r.RunID, dir, r.Verdict)
	return code
}
