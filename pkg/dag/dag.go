package dag

import (
	"fmt"
	"sort"
	"strings"

	"github.com/Aetherx86/Scapolite/pkg/evm"
)

// ConflictType denotes the nature of the state conflict between two transactions.
type ConflictType string

const (
	ConflictRAW   ConflictType = "RAW"   // Read-After-Write (Data dependency)
	ConflictWAR   ConflictType = "WAR"   // Write-After-Read (Anti-dependency)
	ConflictWAW   ConflictType = "WAW"   // Write-After-Write (Output dependency)
	ConflictNonce ConflictType = "NONCE" // Same sender nonce ordering
)

// Dependency represents a directed edge in the DAG (FromTx must execute before ToTx).
type Dependency struct {
	FromTx string       `json:"from"`
	ToTx   string       `json:"to"`
	Type   ConflictType `json:"type"`
	Key    string       `json:"key,omitempty"`
}

// ExecutionWave contains transactions that have no mutually conflicting state dependencies
// and can safely be dispatched concurrently across worker threads.
type ExecutionWave struct {
	Index        int      `json:"wave_index"`
	Transactions []string `json:"transactions"`
}

// ConflictDAG models transaction dependencies as a Directed Acyclic Graph.
type ConflictDAG struct {
	Transactions map[string]*evm.Transaction
	Order        []string // Original submission sequence
	Adj          map[string][]Dependency
	InDegree     map[string]int
	Dependencies []Dependency
}

// NewConflictDAG constructs a DAG from a slice of transactions in chronological or mempool arrival order.
func NewConflictDAG(txs []*evm.Transaction) *ConflictDAG {
	dag := &ConflictDAG{
		Transactions: make(map[string]*evm.Transaction),
		Order:        make([]string, 0, len(txs)),
		Adj:          make(map[string][]Dependency),
		InDegree:     make(map[string]int),
		Dependencies: make([]Dependency, 0),
	}

	for _, tx := range txs {
		dag.Transactions[tx.Hash] = tx
		dag.Order = append(dag.Order, tx.Hash)
		dag.InDegree[tx.Hash] = 0
		dag.Adj[tx.Hash] = make([]Dependency, 0)
	}

	dag.buildDependencies()
	return dag
}

// buildDependencies detects RAW, WAR, WAW, and Nonce dependencies across all transaction pairs.
func (d *ConflictDAG) buildDependencies() {
	edgeSet := make(map[string]struct{})

	for i := 0; i < len(d.Order); i++ {
		txA := d.Transactions[d.Order[i]]

		for j := i + 1; j < len(d.Order); j++ {
			txB := d.Transactions[d.Order[j]]
			edgeKey := txA.Hash + "->" + txB.Hash

			// 1. Same sender nonce ordering constraint
			if txA.From == txB.From {
				if _, exists := edgeSet[edgeKey]; !exists {
					dep := Dependency{
						FromTx: txA.Hash,
						ToTx:   txB.Hash,
						Type:   ConflictNonce,
						Key:    "nonce:" + txA.From,
					}
					d.addEdge(dep)
					edgeSet[edgeKey] = struct{}{}
					continue
				}
			}

			// 2. State slot conflicts (RAW, WAR, WAW)
			var conflictFound bool
			var dep Dependency

			// RAW: TxA writes to slot that TxB reads
			for slot := range txA.WriteSet {
				if _, reads := txB.ReadSet[slot]; reads {
					dep = Dependency{FromTx: txA.Hash, ToTx: txB.Hash, Type: ConflictRAW, Key: slot}
					conflictFound = true
					break
				}
			}

			// WAR: TxA reads slot that TxB writes
			if !conflictFound {
				for slot := range txA.ReadSet {
					if _, writes := txB.WriteSet[slot]; writes {
						dep = Dependency{FromTx: txA.Hash, ToTx: txB.Hash, Type: ConflictWAR, Key: slot}
						conflictFound = true
						break
					}
				}
			}

			// WAW: Both TxA and TxB write to the same slot
			if !conflictFound {
				for slot := range txA.WriteSet {
					if _, writes := txB.WriteSet[slot]; writes {
						dep = Dependency{FromTx: txA.Hash, ToTx: txB.Hash, Type: ConflictWAW, Key: slot}
						conflictFound = true
						break
					}
				}
			}

			if conflictFound {
				if _, exists := edgeSet[edgeKey]; !exists {
					d.addEdge(dep)
					edgeSet[edgeKey] = struct{}{}
				}
			}
		}
	}
}

func (d *ConflictDAG) addEdge(dep Dependency) {
	d.Adj[dep.FromTx] = append(d.Adj[dep.FromTx], dep)
	d.InDegree[dep.ToTx]++
	d.Dependencies = append(d.Dependencies, dep)
}

// PartitionWaves executes a Kahn-style topological partitioning to compute parallel execution waves.
// All transactions within a single wave can execute concurrently without lock contention or rollbacks.
func (d *ConflictDAG) PartitionWaves() ([]ExecutionWave, error) {
	inDegree := make(map[string]int, len(d.InDegree))
	for k, v := range d.InDegree {
		inDegree[k] = v
	}

	var waves []ExecutionWave
	processedCount := 0

	for {
		var currentWave []string
		for _, txHash := range d.Order {
			if inDegree[txHash] == 0 {
				currentWave = append(currentWave, txHash)
			}
		}

		if len(currentWave) == 0 {
			break
		}

		// Sort deterministically by original submission order
		sort.Slice(currentWave, func(i, j int) bool {
			return currentWave[i] < currentWave[j]
		})

		waves = append(waves, ExecutionWave{
			Index:        len(waves),
			Transactions: currentWave,
		})

		for _, txHash := range currentWave {
			inDegree[txHash] = -1 // Mark as scheduled
			processedCount++

			for _, edge := range d.Adj[txHash] {
				inDegree[edge.ToTx]--
			}
		}
	}

	if processedCount < len(d.Order) {
		return nil, fmt.Errorf("cycle detected in transaction conflict DAG (%d of %d scheduled)", processedCount, len(d.Order))
	}

	return waves, nil
}

// ScheduleMetrics summarizes the concurrency potential and speedup over sequential execution.
type ScheduleMetrics struct {
	TotalTransactions int     `json:"total_transactions"`
	TotalDependencies int     `json:"total_dependencies"`
	TotalWaves        int     `json:"total_waves"`
	MaxConcurrency    int     `json:"max_concurrency"`
	TheoreticalSpeedup float64 `json:"theoretical_speedup"`
	CriticalPathLen   int     `json:"critical_path_length"`
}

// ComputeMetrics evaluates performance metrics for the scheduled execution waves.
func (d *ConflictDAG) ComputeMetrics(waves []ExecutionWave) ScheduleMetrics {
	total := len(d.Order)
	if total == 0 {
		return ScheduleMetrics{}
	}

	maxConc := 0
	for _, w := range waves {
		if len(w.Transactions) > maxConc {
			maxConc = len(w.Transactions)
		}
	}

	speedup := 1.0
	if len(waves) > 0 {
		speedup = float64(total) / float64(len(waves))
	}

	return ScheduleMetrics{
		TotalTransactions:  total,
		TotalDependencies:  len(d.Dependencies),
		TotalWaves:         len(waves),
		MaxConcurrency:     maxConc,
		TheoreticalSpeedup: speedup,
		CriticalPathLen:    len(waves),
	}
}

// ConflictSummary returns human-readable details of a dependency edge.
func (dep Dependency) String() string {
	keyDisp := dep.Key
	if len(keyDisp) > 30 {
		keyDisp = keyDisp[:14] + "..." + keyDisp[len(keyDisp)-12:]
	}
	return fmt.Sprintf("%s -> %s [%s] on %s",
		shortHash(dep.FromTx), shortHash(dep.ToTx), dep.Type, keyDisp)
}

func shortHash(h string) string {
	if len(h) <= 10 {
		return h
	}
	if strings.HasPrefix(h, "0x") {
		return h[:8] + ".."
	}
	return h[:6] + ".."
}
