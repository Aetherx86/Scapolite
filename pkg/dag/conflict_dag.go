package dag

import (
	"fmt"
	"math/big"
	"strings"

	"github.com/omakasekim/Scapolite/pkg/evm"
)

// ConflictType denotes the category of state or ordering conflict between two EVM transactions.
type ConflictType string

const (
	ConflictRAW          ConflictType = "RAW"               // Read-After-Write (Data dependency)
	ConflictWAR          ConflictType = "WAR"               // Write-After-Read (Anti-dependency)
	ConflictWAW          ConflictType = "WAW"               // Write-After-Write (Output dependency)
	ConflictNonce        ConflictType = "NONCE"             // Same sender sequential nonce ordering
	ConflictNonceDup     ConflictType = "NONCE_CONFLICT"    // Same sender duplicate nonce (RBF conflict)
	ConflictBalance      ConflictType = "BALANCE"           // Cross-transaction account balance dependency
	ConflictBalanceDrain ConflictType = "BALANCE_VIOLATION" // Insufficient balance / overdraft hazard
)

// Dependency represents a directed edge in the DAG (FromTx must execute before ToTx).
type Dependency struct {
	FromTx string       `json:"from"`
	ToTx   string       `json:"to"`
	Type   ConflictType `json:"type"`
	Key    string       `json:"key,omitempty"`
}

// String returns a concise human-readable description of a dependency edge.
func (dep Dependency) String() string {
	keyDisp := dep.Key
	if len(keyDisp) > 36 {
		keyDisp = keyDisp[:16] + "..." + keyDisp[len(keyDisp)-14:]
	}
	return fmt.Sprintf("%s -> %s [%s] on %s",
		shortHash(dep.FromTx), shortHash(dep.ToTx), dep.Type, keyDisp)
}

// ExecutionWave holds mutually independent transactions that can execute concurrently
// across parallel EVM workers without lock contention or state rollbacks.
type ExecutionWave struct {
	Index         int      `json:"wave_index"`
	TxCount       int      `json:"tx_count"`
	TotalGas      uint64   `json:"total_gas"`
	MaxTxGas      uint64   `json:"max_tx_gas"`
	LoadImbalance float64  `json:"load_imbalance"`
	Transactions  []string `json:"transactions"`
}

// ScheduleMetrics summarizes concurrency, critical-path bounds, hazard counts, and efficiency.
type ScheduleMetrics struct {
	TotalTransactions  int                  `json:"total_transactions"`
	TotalDependencies  int                  `json:"total_dependencies"`
	TotalWaves         int                  `json:"total_waves"`
	MaxConcurrency     int                  `json:"max_concurrency"`
	TheoreticalSpeedup float64              `json:"theoretical_speedup"`
	CriticalPathLen    int                  `json:"critical_path_length"`
	TotalGas           uint64               `json:"total_gas"`
	CriticalPathGas    uint64               `json:"critical_path_gas"`
	GasWeightedSpeedup float64              `json:"gas_weighted_speedup"`
	WaveGasSpeedup     float64              `json:"wave_gas_speedup"`
	ParallelEfficiency float64              `json:"parallel_efficiency"`
	StateConflicts     int                  `json:"state_conflicts"`
	NonceDependencies  int                  `json:"nonce_dependencies"`
	BalanceHazards     int                  `json:"balance_hazards"`
	BalanceViolations  int                  `json:"balance_violations"`
	NonceConflicts     int                  `json:"nonce_conflicts"`
	NonceGaps          int                  `json:"nonce_gaps"`
	HazardBreakdown    map[ConflictType]int `json:"hazard_breakdown,omitempty"`
}

// ConflictDAG models EVM transaction state dependencies as a Directed Acyclic Graph.
type ConflictDAG struct {
	Transactions map[string]*evm.Transaction
	Order        []string // Canonical execution order (nonce-consistent per sender)
	Adj          map[string][]Dependency
	InDegree     map[string]int
	Dependencies []Dependency
	Mode         string

	scheduler *Scheduler
	report    *ConflictReport
}

// NewConflictDAG constructs a deep-mode (storage-slot + balance + nonce) conflict DAG.
func NewConflictDAG(txs []*evm.Transaction) *ConflictDAG {
	return NewConflictDAGWithBalances(txs, "deep", nil)
}

// NewConflictDAGWithMode constructs a conflict DAG in either "fast" (contract-level)
// or "deep" (fine-grained storage-slot) scheduling mode.
func NewConflictDAGWithMode(txs []*evm.Transaction, mode string) *ConflictDAG {
	return NewConflictDAGWithBalances(txs, mode, nil)
}

// NewConflictDAGWithBalances constructs a conflict DAG with optional initial account balances
// (in wei) to detect overdraft and balance-drain hazards alongside storage slot conflicts.
func NewConflictDAGWithBalances(txs []*evm.Transaction, mode string, initialBalances map[string]*big.Int) *ConflictDAG {
	normMode := normalizeMode(mode)
	sched := NewScheduler(normMode)
	if len(initialBalances) > 0 {
		sched.SetAccountBalances(initialBalances)
	}

	d := &ConflictDAG{
		Transactions: make(map[string]*evm.Transaction, len(txs)),
		Order:        make([]string, 0, len(txs)),
		Adj:          make(map[string][]Dependency, len(txs)),
		InDegree:     make(map[string]int, len(txs)),
		Dependencies: make([]Dependency, 0),
		Mode:         normMode,
		scheduler:    sched,
	}

	for _, tx := range txs {
		if tx == nil {
			continue
		}
		if tx.ReadSet == nil || tx.WriteSet == nil {
			tx.DeriveStateAccess()
		}
		if _, exists := d.Transactions[tx.Hash]; exists {
			continue
		}
		d.Transactions[tx.Hash] = tx
		d.InDegree[tx.Hash] = 0
		d.Adj[tx.Hash] = make([]Dependency, 0)
		sched.AddEVMTransaction(tx)
	}

	d.report = sched.BuildGraph()

	// Snapshot canonical order and edges from the underlying scheduler.
	sched.mu.RLock()
	for _, tx := range sched.ordered {
		d.Order = append(d.Order, tx.Hash)
	}
	for _, e := range sched.edges {
		dep := Dependency{
			FromTx: e.FromTx,
			ToTx:   e.ToTx,
			Type:   mapDependencyToConflict(e.Type),
			Key:    e.Key,
		}
		d.Adj[dep.FromTx] = append(d.Adj[dep.FromTx], dep)
		d.InDegree[dep.ToTx]++
		d.Dependencies = append(d.Dependencies, dep)
	}
	sched.mu.RUnlock()

	return d
}

// PartitionWaves executes Kahn topological partitioning to compute parallel execution waves.
func (d *ConflictDAG) PartitionWaves() ([]ExecutionWave, error) {
	rawWaves, err := d.scheduler.ComputeParallelWaves()
	if err != nil {
		return nil, err
	}

	waves := make([]ExecutionWave, len(rawWaves))
	for i, w := range rawWaves {
		hashes := make([]string, len(w))
		var totalGas, maxGas uint64
		for j, tx := range w {
			hashes[j] = tx.Hash
			g := tx.EffectiveGasLimit()
			totalGas += g
			if g > maxGas {
				maxGas = g
			}
		}
		var imbalance float64
		if len(w) > 0 && totalGas > 0 {
			avg := float64(totalGas) / float64(len(w))
			imbalance = float64(maxGas) / avg
		}
		waves[i] = ExecutionWave{
			Index:         i,
			TxCount:       len(w),
			TotalGas:      totalGas,
			MaxTxGas:      maxGas,
			LoadImbalance: imbalance,
			Transactions:  hashes,
		}
	}

	return waves, nil
}

// ComputeMetrics returns comprehensive concurrency, gas-weighted speedup, and hazard metrics.
func (d *ConflictDAG) ComputeMetrics(waves []ExecutionWave) ScheduleMetrics {
	total := len(d.Order)
	if total == 0 {
		return ScheduleMetrics{}
	}

	maxConc := 0
	var totalGas, waveBarrierGas uint64
	for _, w := range waves {
		if len(w.Transactions) > maxConc {
			maxConc = len(w.Transactions)
		}
		totalGas += w.TotalGas
		waveBarrierGas += w.MaxTxGas
	}
	if totalGas == 0 && d.report != nil {
		totalGas = d.report.TotalGas
	}

	speedup := 1.0
	if len(waves) > 0 {
		speedup = float64(total) / float64(len(waves))
	}

	cp, _ := d.scheduler.ComputeCriticalPath()

	var gasSpeedup, waveGasSpeedup, efficiency float64
	if totalGas > 0 && cp.GasWeight > 0 {
		gasSpeedup = float64(totalGas) / float64(cp.GasWeight)
	}
	if totalGas > 0 && waveBarrierGas > 0 {
		waveGasSpeedup = float64(totalGas) / float64(waveBarrierGas)
	}
	if maxConc > 0 && waveBarrierGas > 0 {
		efficiency = float64(totalGas) / (float64(waveBarrierGas) * float64(maxConc))
	}

	breakdown := make(map[ConflictType]int)
	if d.report != nil {
		for k, v := range d.report.HazardBreakdown {
			breakdown[mapDependencyToConflict(k)] += v
		}
	}

	m := ScheduleMetrics{
		TotalTransactions:  total,
		TotalDependencies:  len(d.Dependencies),
		TotalWaves:         len(waves),
		MaxConcurrency:     maxConc,
		TheoreticalSpeedup: speedup,
		CriticalPathLen:    cp.HopLength,
		TotalGas:           totalGas,
		CriticalPathGas:    cp.GasWeight,
		GasWeightedSpeedup: gasSpeedup,
		WaveGasSpeedup:     waveGasSpeedup,
		ParallelEfficiency: efficiency,
		HazardBreakdown:    breakdown,
	}
	if m.CriticalPathLen == 0 {
		m.CriticalPathLen = len(waves)
	}
	if d.report != nil {
		m.StateConflicts = d.report.StateConflicts
		m.NonceDependencies = d.report.NonceDependencies
		m.BalanceHazards = d.report.BalanceHazards
		m.BalanceViolations = d.report.BalanceViolations
		m.NonceConflicts = d.report.NonceConflicts
		m.NonceGaps = d.report.NonceGaps
	}

	return m
}

// ComputeCriticalPath extracts both hop-based and gas-weighted critical chains.
func (d *ConflictDAG) ComputeCriticalPath() CriticalPath {
	cp, _ := d.scheduler.ComputeCriticalPath()
	return cp
}

// SimulateWorkers simulates fixed-worker wave-barrier and dynamic work-stealing execution.
func (d *ConflictDAG) SimulateWorkers(numWorkers int) WorkerSimulation {
	sim, _ := d.scheduler.SimulateWorkers(numWorkers)
	return sim
}

// ComputeHotspots returns the top contended storage slots and accounts in the DAG.
func (d *ConflictDAG) ComputeHotspots(limit int) []ContentionHotspot {
	return d.scheduler.ComputeHotspots(limit)
}

// NonceAnomalies returns duplicate-nonce and nonce-gap hazards found in the batch.
func (d *ConflictDAG) NonceAnomalies() []NonceAnomaly {
	return d.scheduler.NonceAnomalies()
}

// BalanceViolations returns overdraft / balance-drain hazards found in the batch.
func (d *ConflictDAG) BalanceViolations() []BalanceViolation {
	return d.scheduler.BalanceViolations()
}

// Report returns the underlying ConflictReport computed during DAG construction.
func (d *ConflictDAG) Report() *ConflictReport {
	if d.report == nil {
		return &ConflictReport{}
	}
	cp := *d.report
	return &cp
}

func mapDependencyToConflict(t DependencyType) ConflictType {
	switch t {
	case DepReadWrite:
		return ConflictRAW
	case DepWriteRead:
		return ConflictWAR
	case DepWriteWrite:
		return ConflictWAW
	case DepNonce:
		return ConflictNonce
	case DepNonceConflict:
		return ConflictNonceDup
	case DepBalance:
		return ConflictBalance
	case DepBalanceDrain:
		return ConflictBalanceDrain
	default:
		return ConflictType(t)
	}
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
