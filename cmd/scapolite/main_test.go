package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCLIVersion(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"-version"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("expected exit 0, got %d (stderr: %s)", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "Scapolite "+version) {
		t.Fatalf("unexpected version output: %q", stdout.String())
	}
}

func TestCLIDemoTextAndJSON(t *testing.T) {
	// Text mode (deep)
	var stdout, stderr bytes.Buffer
	code := run([]string{"-demo", "25", "-mode", "deep", "-workers", "4"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("demo deep failed with %d: %s", code, stderr.String())
	}
	outStr := stdout.String()
	for _, want := range []string{
		"EVM Dependency DAG Scheduler [deep mode]",
		"Critical Path:",
		"Worker Simulation:",
		"Parallel Execution Waves:",
		"SANDWICH_ATTACK",
	} {
		if !strings.Contains(outStr, want) {
			t.Errorf("expected demo text output to contain %q\nOutput:\n%s", want, outStr)
		}
	}

	// JSON mode (deep)
	stdout.Reset()
	stderr.Reset()
	code = run([]string{"-demo", "30", "-mode", "deep", "-workers", "8", "-json"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("demo json failed with %d: %s", code, stderr.String())
	}

	var parsed OutputJSON
	if err := json.Unmarshal(stdout.Bytes(), &parsed); err != nil {
		t.Fatalf("invalid JSON output: %v\nRaw:\n%s", err, stdout.String())
	}
	if parsed.Metrics.TotalTransactions != 30 {
		t.Errorf("expected 30 transactions, got %d", parsed.Metrics.TotalTransactions)
	}
	if parsed.Metrics.TotalWaves == 0 || len(parsed.Waves) != parsed.Metrics.TotalWaves {
		t.Errorf("wave count mismatch: metrics=%d, waves=%d", parsed.Metrics.TotalWaves, len(parsed.Waves))
	}
	if parsed.CriticalPath.GasWeight == 0 || len(parsed.CriticalPath.GasChain) == 0 {
		t.Errorf("expected populated critical path, got %+v", parsed.CriticalPath)
	}
	if parsed.WorkerSimulation.Workers != 8 || parsed.WorkerSimulation.WorkStealingSpeedup <= 0 {
		t.Errorf("expected valid 8-core worker simulation, got %+v", parsed.WorkerSimulation)
	}
	if len(parsed.Alerts) == 0 {
		t.Errorf("expected MEV alerts in deep demo mode")
	}
}

func TestCLIGenerateExportAndIngestRoundTrip(t *testing.T) {
	dir := t.TempDir()
	mempoolFile := filepath.Join(dir, "mempool.json")
	scheduleFile := filepath.Join(dir, "schedule.json")

	// Step 1: Generate and export mempool JSON.
	var stdout, stderr bytes.Buffer
	code := run([]string{"-gen", "40", "-seed", "123", "-out", mempoolFile, "-mode", "fast"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("-gen failed with %d: %s", code, stderr.String())
	}
	if _, err := os.Stat(mempoolFile); err != nil {
		t.Fatalf("expected exported mempool file: %v", err)
	}

	// Step 2: Re-ingest exported mempool file and write schedule report to scheduleFile.
	stdout.Reset()
	stderr.Reset()
	code = run([]string{"-f", mempoolFile, "-mode", "deep", "-json", "-out", scheduleFile}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("-f ingest failed with %d: %s", code, stderr.String())
	}

	rawReport, err := os.ReadFile(scheduleFile)
	if err != nil {
		t.Fatalf("expected exported schedule JSON: %v", err)
	}
	var parsed OutputJSON
	if err := json.Unmarshal(rawReport, &parsed); err != nil {
		t.Fatalf("invalid schedule JSON file: %v", err)
	}
	if parsed.Metrics.TotalTransactions != 40 {
		t.Fatalf("expected 40 transactions after round-trip, got %d", parsed.Metrics.TotalTransactions)
	}
}

func TestCLIBenchmarkTextAndJSON(t *testing.T) {
	// Text benchmark
	var stdout, stderr bytes.Buffer
	code := run([]string{"-bench", "50,100", "-workers", "4"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("-bench text failed with %d: %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "EVM DAG Scheduler Benchmark") {
		t.Fatalf("unexpected benchmark output:\n%s", stdout.String())
	}

	// JSON benchmark
	stdout.Reset()
	stderr.Reset()
	code = run([]string{"-bench", "50,100", "-workers", "4", "-json"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("-bench json failed with %d: %s", code, stderr.String())
	}
	var report BenchmarkReport
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("invalid benchmark JSON: %v", err)
	}
	if len(report.Results) != 4 { // 2 batch sizes x 2 modes (fast, deep)
		t.Fatalf("expected 4 benchmark rows, got %d", len(report.Results))
	}
	if report.Workers != 4 {
		t.Fatalf("expected workers=4, got %d", report.Workers)
	}
}

func TestCLIValidationErrors(t *testing.T) {
	cases := [][]string{
		{"-mode", "invalid"},
		{"-workers", "0"},
		{"-demo", "0"},
		{"-bench", "abc"},
		{"-f", "/nonexistent/path/mempool.json"},
	}
	for _, args := range cases {
		var stdout, stderr bytes.Buffer
		if code := run(args, &stdout, &stderr); code == 0 {
			t.Errorf("expected non-zero exit code for args %v", args)
		}
	}
}
