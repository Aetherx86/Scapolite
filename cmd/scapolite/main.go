package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Aetherx86/Scapolite/pkg/dag"
	"github.com/Aetherx86/Scapolite/pkg/evm"
)

const version = "0.2.0"

// OutputJSON is the structured CLI report format for block builder pipelines.
type OutputJSON struct {
	Version           string                  `json:"version"`
	Mode              string                  `json:"mode"`
	ScheduleLatencyUs int64                   `json:"schedule_latency_us"`
	Metrics           dag.ScheduleMetrics     `json:"metrics"`
	CriticalPath      dag.CriticalPath        `json:"critical_path"`
	WorkerSimulation  dag.WorkerSimulation    `json:"worker_simulation"`
	Waves             []dag.ExecutionWave     `json:"waves"`
	Hotspots          []dag.ContentionHotspot `json:"hotspots,omitempty"`
	NonceAnomalies    []dag.NonceAnomaly      `json:"nonce_anomalies,omitempty"`
	BalanceViolations []dag.BalanceViolation  `json:"balance_violations,omitempty"`
	Alerts            []dag.MEVAlert          `json:"mev_alerts,omitempty"`
}

// BenchmarkRow captures timing and parallel efficiency metrics for a single benchmark point.
type BenchmarkRow struct {
	BatchSize           int     `json:"batch_size"`
	Mode                string  `json:"mode"`
	BuildLatencyUs      int64   `json:"build_latency_us"`
	PartitionLatencyUs  int64   `json:"partition_latency_us"`
	TotalLatencyUs      int64   `json:"total_latency_us"`
	ThroughputTxPerSec  float64 `json:"throughput_tx_per_sec"`
	TotalDependencies   int     `json:"total_dependencies"`
	TotalWaves          int     `json:"total_waves"`
	MaxConcurrency      int     `json:"max_concurrency"`
	TheoreticalSpeedup  float64 `json:"theoretical_speedup"`
	GasWeightedSpeedup  float64 `json:"gas_weighted_speedup"`
	WaveBarrierSpeedup  float64 `json:"wave_barrier_speedup"`
	WorkStealingSpeedup float64 `json:"work_stealing_speedup"`
	WorkStealUtil       float64 `json:"work_steal_utilization"`
}

// BenchmarkReport is the JSON output format for `-bench`.
type BenchmarkReport struct {
	Version string         `json:"version"`
	Workers int            `json:"workers"`
	Seed    int64          `json:"seed"`
	Results []BenchmarkRow `json:"results"`
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("scapolite", flag.ContinueOnError)
	fs.SetOutput(stderr)

	filePath := fs.String("f", "", "Path to mempool transactions JSON file")
	demoCount := fs.Int("demo", 25, "Number of synthetic transactions to generate in demo mode")
	genCount := fs.Int("gen", 0, "Generate deterministic synthetic mempool with N transactions")
	outPath := fs.String("out", "", "Export generated mempool (with -gen) or schedule JSON to file")
	seed := fs.Int64("seed", 42, "Deterministic PRNG seed for synthetic workloads")
	mode := fs.String("mode", "deep", "Scheduling procedure: 'fast' (address-level) or 'deep' (storage slots + MEV)")
	workers := fs.Int("workers", 8, "Simulated parallel EVM worker cores for efficiency metrics")
	benchSpec := fs.String("bench", "", "Run built-in scheduler benchmark for comma-separated batch sizes (e.g. 100,500,1000)")
	jsonOutput := fs.Bool("json", false, "Output structured JSON report")
	showVersion := fs.Bool("version", false, "Print version and exit")

	fs.Usage = func() {
		fmt.Fprintf(stderr, "Scapolite v%s - EVM Mempool Transaction Dependency DAG Scheduler\n\n", version)
		fmt.Fprintf(stderr, "Usage: scapolite [options]\n\n")
		fmt.Fprintf(stderr, "Options:\n")
		fs.PrintDefaults()
		fmt.Fprintf(stderr, "\nDual Procedures:\n")
		fmt.Fprintf(stderr, "  -mode=fast   Coarse-grained contract address contention for minimal scheduling latency.\n")
		fmt.Fprintf(stderr, "  -mode=deep   Fine-grained storage slot (SSTORE/SLOAD), balance/nonce hazard DAG, critical-path analysis, and MEV detection.\n")
	}

	if err := fs.Parse(args); err != nil {
		return 2
	}

	if *showVersion {
		fmt.Fprintf(stdout, "Scapolite %s (Aether Systems & Research Lab)\n", version)
		return 0
	}

	normMode := strings.ToLower(strings.TrimSpace(*mode))
	if normMode != "fast" && normMode != "deep" {
		fmt.Fprintf(stderr, "Invalid mode %q: expected 'fast' or 'deep'\n", *mode)
		return 2
	}
	if *workers < 1 {
		fmt.Fprintf(stderr, "Invalid workers %d: must be >= 1\n", *workers)
		return 2
	}

	if *benchSpec != "" {
		return runBenchmark(*benchSpec, *workers, *seed, *jsonOutput, stdout, stderr)
	}

	var txs []*evm.Transaction
	if *genCount > 0 {
		txs = dag.GenerateSyntheticEVMMempool(*genCount, *seed)
		if *outPath != "" {
			if err := dag.ExportEVMMempoolFile(*outPath, txs); err != nil {
				fmt.Fprintf(stderr, "Failed to export mempool: %v\n", err)
				return 1
			}
			if !*jsonOutput {
				fmt.Fprintf(stdout, "Exported %d synthetic transactions to %s\n", len(txs), *outPath)
			}
		}
	} else if *filePath != "" {
		f, err := os.Open(*filePath)
		if err != nil {
			fmt.Fprintf(stderr, "Error opening file: %v\n", err)
			return 1
		}
		txs, err = dag.IngestEVMMempoolJSON(f)
		_ = f.Close()
		if err != nil {
			fmt.Fprintf(stderr, "Failed to parse JSON transactions: %v\n", err)
			return 1
		}
	} else {
		if *demoCount <= 0 {
			fmt.Fprintf(stderr, "Invalid demo count %d: must be > 0\n", *demoCount)
			return 2
		}
		txs = dag.GenerateSyntheticEVMMempool(*demoCount, *seed)
	}

	start := time.Now()
	conflictDAG := dag.NewConflictDAGWithMode(txs, normMode)
	waves, err := conflictDAG.PartitionWaves()
	elapsed := time.Since(start)
	if err != nil {
		fmt.Fprintf(stderr, "Scheduling error: %v\n", err)
		return 1
	}

	metrics := conflictDAG.ComputeMetrics(waves)
	critPath := conflictDAG.ComputeCriticalPath()
	workerSim := conflictDAG.SimulateWorkers(*workers)
	hotspots := conflictDAG.ComputeHotspots(5)
	nonceAnomalies := conflictDAG.NonceAnomalies()
	balViolations := conflictDAG.BalanceViolations()

	var alerts []dag.MEVAlert
	if normMode == "deep" {
		detector := dag.NewMEVDetector(conflictDAG)
		alerts = detector.DetectThreats()
	}

	out := OutputJSON{
		Version:           version,
		Mode:              normMode,
		ScheduleLatencyUs: elapsed.Microseconds(),
		Metrics:           metrics,
		CriticalPath:      critPath,
		WorkerSimulation:  workerSim,
		Waves:             waves,
		Hotspots:          hotspots,
		NonceAnomalies:    nonceAnomalies,
		BalanceViolations: balViolations,
		Alerts:            alerts,
	}

	// If -out was supplied without -gen, export the JSON schedule report to disk.
	if *genCount == 0 && *outPath != "" {
		if err := writeJSONFile(*outPath, out); err != nil {
			fmt.Fprintf(stderr, "Failed to write schedule output: %v\n", err)
			return 1
		}
	}

	if *jsonOutput {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(out); err != nil {
			fmt.Fprintf(stderr, "Failed to encode JSON output: %v\n", err)
			return 1
		}
		return 0
	}

	printTextReport(stdout, out, elapsed)
	return 0
}

func runBenchmark(spec string, workers int, seed int64, jsonOutput bool, stdout, stderr io.Writer) int {
	parts := strings.Split(spec, ",")
	sizes := make([]int, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		n, err := strconv.Atoi(p)
		if err != nil || n <= 0 {
			fmt.Fprintf(stderr, "Invalid benchmark batch size %q: must be a positive integer\n", p)
			return 2
		}
		sizes = append(sizes, n)
	}
	if len(sizes) == 0 {
		fmt.Fprintf(stderr, "Benchmark specification must contain at least one positive batch size\n")
		return 2
	}

	rows := make([]BenchmarkRow, 0, len(sizes)*2)
	for _, size := range sizes {
		txs := dag.GenerateSyntheticEVMMempool(size, seed)
		for _, m := range []string{"fast", "deep"} {
			t0 := time.Now()
			d := dag.NewConflictDAGWithMode(txs, m)
			buildDur := time.Since(t0)

			t1 := time.Now()
			waves, err := d.PartitionWaves()
			partDur := time.Since(t1)
			if err != nil {
				fmt.Fprintf(stderr, "Benchmark failed for size=%d mode=%s: %v\n", size, m, err)
				return 1
			}

			metrics := d.ComputeMetrics(waves)
			sim := d.SimulateWorkers(workers)
			totalDur := buildDur + partDur
			var tps float64
			if totalDur > 0 {
				tps = float64(size) / totalDur.Seconds()
			}

			rows = append(rows, BenchmarkRow{
				BatchSize:           size,
				Mode:                m,
				BuildLatencyUs:      buildDur.Microseconds(),
				PartitionLatencyUs:  partDur.Microseconds(),
				TotalLatencyUs:      totalDur.Microseconds(),
				ThroughputTxPerSec:  tps,
				TotalDependencies:   metrics.TotalDependencies,
				TotalWaves:          metrics.TotalWaves,
				MaxConcurrency:      metrics.MaxConcurrency,
				TheoreticalSpeedup:  metrics.TheoreticalSpeedup,
				GasWeightedSpeedup:  metrics.GasWeightedSpeedup,
				WaveBarrierSpeedup:  sim.WaveBarrierSpeedup,
				WorkStealingSpeedup: sim.WorkStealingSpeedup,
				WorkStealUtil:       sim.WorkStealUtilization,
			})
		}
	}

	if jsonOutput {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(BenchmarkReport{
			Version: version,
			Workers: workers,
			Seed:    seed,
			Results: rows,
		})
		return 0
	}

	fmt.Fprintf(stdout, "Scapolite v%s - EVM DAG Scheduler Benchmark (workers=%d, seed=%d)\n", version, workers, seed)
	fmt.Fprintln(stdout, strings.Repeat("-", 114))
	fmt.Fprintf(stdout, "%-8s %-6s %10s %10s %11s %7s %7s %9s %9s %9s %9s\n",
		"Batch", "Mode", "Build(us)", "Wave(us)", "Tx/sec", "Waves", "MaxC", "GasSpdup", "WaveSpd", "StealSpd", "Util(%)")
	fmt.Fprintln(stdout, strings.Repeat("-", 114))
	for _, r := range rows {
		fmt.Fprintf(stdout, "%-8d %-6s %10d %10d %11.0f %7d %7d %8.2fx %8.2fx %8.2fx %8.1f%%\n",
			r.BatchSize, r.Mode, r.BuildLatencyUs, r.PartitionLatencyUs, r.ThroughputTxPerSec,
			r.TotalWaves, r.MaxConcurrency, r.GasWeightedSpeedup, r.WaveBarrierSpeedup, r.WorkStealingSpeedup, r.WorkStealUtil*100)
	}
	fmt.Fprintln(stdout, strings.Repeat("-", 114))
	return 0
}

func printTextReport(w io.Writer, out OutputJSON, elapsed time.Duration) {
	m := out.Metrics
	sim := out.WorkerSimulation

	fmt.Fprintf(w, "Scapolite v%s - EVM Dependency DAG Scheduler [%s mode]\n", out.Version, out.Mode)
	fmt.Fprintf(w, "Scheduled %d transactions (%d gas) into %d parallel waves in %s\n",
		m.TotalTransactions, m.TotalGas, m.TotalWaves, elapsed.Round(time.Microsecond))
	fmt.Fprintln(w, strings.Repeat("-", 80))
	fmt.Fprintf(w, "Max Concurrency:      %d concurrent tx/wave\n", m.MaxConcurrency)
	fmt.Fprintf(w, "Theoretical Speedup:  %.2fx (tx-count) | %.2fx (gas-weighted critical path)\n",
		m.TheoreticalSpeedup, m.GasWeightedSpeedup)
	fmt.Fprintf(w, "Wave Gas Efficiency:  %.2fx wave-barrier speedup | %.1f%% peak-width efficiency\n",
		m.WaveGasSpeedup, m.ParallelEfficiency*100)
	fmt.Fprintf(w, "Worker Simulation:    %d cores -> %.2fx wave-barrier (%.1f%% util) | %.2fx work-stealing (%.1f%% util)\n",
		sim.Workers, sim.WaveBarrierSpeedup, sim.WaveUtilization*100, sim.WorkStealingSpeedup, sim.WorkStealUtilization*100)
	fmt.Fprintf(w, "Dependencies:         %d total (%d storage, %d nonce, %d balance)\n",
		m.TotalDependencies, m.StateConflicts, m.NonceDependencies, m.BalanceHazards)
	fmt.Fprintf(w, "Critical Path:        %d hops | %d gas (%d txs on gas-critical chain)\n",
		out.CriticalPath.HopLength, out.CriticalPath.GasWeight, len(out.CriticalPath.GasChain))
	if m.NonceConflicts > 0 || m.NonceGaps > 0 || m.BalanceViolations > 0 {
		fmt.Fprintf(w, "Hazard Anomalies:     %d duplicate nonces, %d nonce gaps, %d balance violations\n",
			m.NonceConflicts, m.NonceGaps, m.BalanceViolations)
	}
	fmt.Fprintln(w, strings.Repeat("-", 80))

	fmt.Fprintln(w, "\nParallel Execution Waves:")
	for i, wave := range out.Waves {
		if i >= 8 {
			fmt.Fprintf(w, "... and %d more waves\n", len(out.Waves)-i)
			break
		}
		shortHashes := make([]string, 0, len(wave.Transactions))
		for _, h := range wave.Transactions {
			if len(h) > 10 {
				h = h[:10] + ".."
			}
			shortHashes = append(shortHashes, h)
		}
		fmt.Fprintf(w, "  Wave %2d (%2d txs, %7d gas, imbal %.2fx): %s\n",
			i+1, wave.TxCount, wave.TotalGas, wave.LoadImbalance, strings.Join(shortHashes, ", "))
	}

	if len(out.Hotspots) > 0 {
		fmt.Fprintf(w, "\nTop Contention Hotspots (%d):\n", len(out.Hotspots))
		for _, hs := range out.Hotspots {
			fmt.Fprintf(w, "  %s (edges=%d, writes=%d, reads=%d, txs=%d)\n",
				hs.Key, hs.EdgeCount, hs.WriteCount, hs.ReadCount, len(hs.TxHashes))
		}
	}

	if len(out.Alerts) > 0 {
		fmt.Fprintf(w, "\nMEV & Frontrunning Alerts Detected (%d):\n", len(out.Alerts))
		for _, alert := range out.Alerts {
			fmt.Fprintf(w, "  [%s] %s: %s (Key: %s)\n", alert.Severity, alert.Type, alert.Description, alert.ConflictedKey)
		}
	} else if out.Mode == "deep" {
		fmt.Fprintln(w, "\nNo adversarial MEV patterns detected in mempool batch.")
	}
}

func writeJSONFile(path string, v any) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
