package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Aetherx86/Scapolite/pkg/dag"
)

const version = "0.1.0"

type ScheduleOutput struct {
	Report *dag.ConflictReport `json:"report"`
	Waves  [][]string           `json:"waves"`
}

func main() {
	filePath := flag.String("f", "", "Path to mempool transactions JSON file")
	genCount := flag.Int("gen", 0, "Generate synthetic mempool with N transactions")
	outPath := flag.String("out", "", "Export generated mempool or schedule to file")
	mode := flag.String("mode", "deep", "Scheduling procedure: 'fast' (address-level) or 'deep' (SSTORE/SLOAD slots)")
	jsonOutput := flag.Bool("json", false, "Output schedule as structured JSON")
	showVersion := flag.Bool("version", false, "Print version and exit")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Scapolite v%s - EVM Mempool Transaction Dependency DAG Scheduler\n\n", version)
		fmt.Fprintf(os.Stderr, "Usage: scapolite [options]\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nDual Procedures:\n")
		fmt.Fprintf(os.Stderr, "  --mode=fast   Coarse-grained contract and recipient address contention for maximal throughput.\n")
		fmt.Fprintf(os.Stderr, "  --mode=deep   Fine-grained storage slot (SSTORE/SLOAD) conflict graph with topological wave scheduling.\n")
	}

	flag.Parse()

	if *showVersion {
		fmt.Printf("Scapolite %s (Aether Systems & Research Lab)\n", version)
		os.Exit(0)
	}

	var txs []*dag.Transaction
	var err error

	if *genCount > 0 {
		txs = dag.GenerateSyntheticMempool(*genCount, time.Now().UnixNano())
		if *outPath != "" {
			if err := dag.ExportMempoolFile(*outPath, txs); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to export mempool: %v\n", err)
				os.Exit(1)
			}
			if !*jsonOutput {
				fmt.Printf("Exported %d synthetic transactions to %s\n", len(txs), *outPath)
			}
		}
	} else if *filePath != "" {
		f, err := os.Open(*filePath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error opening file: %v\n", err)
			os.Exit(1)
		}
		defer f.Close()
		txs, err = dag.IngestMempoolJSON(f)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading transactions: %v\n", err)
			os.Exit(1)
		}
	} else {
		// Default to synthetic demo batch of 25 transactions
		txs = dag.GenerateSyntheticMempool(25, 42)
	}

	sched := dag.NewScheduler(*mode)
	for _, tx := range txs {
		sched.AddTransaction(tx)
	}

	start := time.Now()
	report := sched.BuildGraph()
	waves, err := sched.ComputeParallelWaves()
	elapsed := time.Since(start)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Scheduling error: %v\n", err)
		os.Exit(1)
	}

	if *jsonOutput {
		var waveHashes [][]string
		for _, w := range waves {
			var hList []string
			for _, tx := range w {
				hList = append(hList, tx.Hash)
			}
			waveHashes = append(waveHashes, hList)
		}
		out := ScheduleOutput{
			Report: report,
			Waves:  waveHashes,
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(out)
		return
	}

	fmt.Printf("Scapolite v%s - EVM Dependency DAG Scheduler [%s mode]\n", version, *mode)
	fmt.Printf("Scheduled %d transactions into %d parallel waves in %s\n",
		report.TotalTransactions, report.IndependentWaves, elapsed.Round(time.Microsecond))
	fmt.Println(strings.Repeat("-", 80))
	fmt.Printf("Max Concurrency:      %d concurrent tx/wave\n", report.MaxConcurrency)
	fmt.Printf("Theoretical Speedup:  %.2fx over sequential EVM\n", report.TheoreticalSpeedup)
	fmt.Printf("State Contention:     %d slot conflicts detected\n", report.StateConflicts)
	fmt.Printf("Nonce Dependencies:   %d sender ordering edges\n", report.NonceDependencies)
	fmt.Println(strings.Repeat("-", 80))

	fmt.Println("\nParallel Execution Waves:")
	for i, w := range waves {
		if i >= 10 {
			fmt.Printf("... and %d more waves\n", len(waves)-i)
			break
		}
		var hashes []string
		for _, tx := range w {
			short := tx.Hash
			if len(short) > 10 {
				short = short[:10] + ".."
			}
			hashes = append(hashes, short)
		}
		fmt.Printf("  Wave %2d (%2d txs): %s\n", i+1, len(w), strings.Join(hashes, ", "))
	}
}
