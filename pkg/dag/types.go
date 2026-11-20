package dag

import (
	"math/big"
	"strings"

	"github.com/Aetherx86/Scapolite/pkg/evm"
)

// DependencyType categorizes directed state or ordering dependencies between transactions.
type DependencyType string

const (
	DepNonce         DependencyType = "NONCE_ORDER"
	DepNonceConflict DependencyType = "NONCE_CONFLICT"
	DepReadWrite     DependencyType = "READ_AFTER_WRITE"
	DepWriteRead     DependencyType = "WRITE_AFTER_READ"
	DepWriteWrite    DependencyType = "WRITE_AFTER_WRITE"
	DepBalance       DependencyType = "BALANCE_HAZARD"
	DepBalanceDrain  DependencyType = "BALANCE_VIOLATION"
)

// Transaction models an EVM transaction with explicit or derived state access footprints.
type Transaction struct {
	Hash         string              `json:"hash"`
	From         string              `json:"from"`
	To           string              `json:"to"`
	Nonce        uint64              `json:"nonce"`
	Value        *big.Int            `json:"value,omitempty"`
	GasPrice     *big.Int            `json:"gas_price,omitempty"`
	MaxPriority  *big.Int            `json:"max_priority_fee_per_gas,omitempty"`
	GasLimit     uint64              `json:"gas_limit,omitempty"`
	Data         []byte              `json:"data,omitempty"`
	AccessList   []evm.AccessTuple   `json:"access_list,omitempty"`
	ReadSlots    map[string][]string `json:"read_slots,omitempty"`
	WriteSlots   map[string][]string `json:"write_slots,omitempty"`
	BalanceDelta map[string]*big.Int `json:"balance_deltas,omitempty"`

	extraBalanceReads  []string
	extraBalanceWrites []string
}

// EffectiveGasLimit returns GasLimit when specified, or a baseline 21,000 gas transfer estimate.
func (tx *Transaction) EffectiveGasLimit() uint64 {
	if tx == nil {
		return 0
	}
	if tx.GasLimit > 0 {
		return tx.GasLimit
	}
	if len(tx.Data) >= 4 || len(tx.WriteSlots) > 0 || len(tx.ReadSlots) > 0 || len(tx.AccessList) > 0 {
		return evm.DefaultContractCallGas
	}
	return evm.DefaultTransferGas
}

// EffectiveGasTip returns the EIP-1559 priority fee if positive, else legacy GasPrice.
func (tx *Transaction) EffectiveGasTip() *big.Int {
	if tx == nil {
		return big.NewInt(0)
	}
	if tx.MaxPriority != nil && tx.MaxPriority.Sign() > 0 {
		return new(big.Int).Set(tx.MaxPriority)
	}
	if tx.GasPrice != nil && tx.GasPrice.Sign() > 0 {
		return new(big.Int).Set(tx.GasPrice)
	}
	return big.NewInt(0)
}

// MaxGasCost computes EffectiveGasLimit * GasPrice, or 0 if GasPrice is unset.
func (tx *Transaction) MaxGasCost() *big.Int {
	if tx == nil || tx.GasPrice == nil || tx.GasPrice.Sign() <= 0 {
		return big.NewInt(0)
	}
	gas := new(big.Int).SetUint64(tx.EffectiveGasLimit())
	return gas.Mul(gas, tx.GasPrice)
}

// FromEVMTransaction adapts an evm.Transaction into a Scheduler Transaction,
// preserving derived storage slots, access lists, balance footprints, and gas metadata.
func FromEVMTransaction(etx *evm.Transaction) *Transaction {
	if etx == nil {
		return nil
	}
	if etx.ReadSet == nil || etx.WriteSet == nil {
		etx.DeriveStateAccess()
	}

	readSlots := make(map[string][]string)
	writeSlots := make(map[string][]string)

	for key := range etx.ReadSet {
		if evm.IsBalanceKey(key) {
			continue
		}
		addr, slot := splitStorageKey(key)
		readSlots[addr] = append(readSlots[addr], slot)
	}
	for key := range etx.WriteSet {
		if evm.IsBalanceKey(key) {
			continue
		}
		addr, slot := splitStorageKey(key)
		writeSlots[addr] = append(writeSlots[addr], slot)
	}

	var balDelta map[string]*big.Int
	if len(etx.BalanceDelta) > 0 {
		balDelta = make(map[string]*big.Int, len(etx.BalanceDelta))
		for k, v := range etx.BalanceDelta {
			if v != nil {
				balDelta[strings.ToLower(k)] = new(big.Int).Set(v)
			}
		}
	}

	var val, gp, mp *big.Int
	if etx.Value != nil {
		val = new(big.Int).Set(etx.Value)
	}
	if etx.GasPrice != nil {
		gp = new(big.Int).Set(etx.GasPrice)
	}
	if etx.MaxPriority != nil {
		mp = new(big.Int).Set(etx.MaxPriority)
	}

	extraBalReads := make([]string, 0, len(etx.BalanceReads))
	for addr := range etx.BalanceReads {
		extraBalReads = append(extraBalReads, addr)
	}
	extraBalWrites := make([]string, 0, len(etx.BalanceWrites))
	for addr := range etx.BalanceWrites {
		extraBalWrites = append(extraBalWrites, addr)
	}

	return &Transaction{
		Hash:               etx.Hash,
		From:               etx.From,
		To:                 etx.To,
		Nonce:              etx.Nonce,
		Value:              val,
		GasPrice:           gp,
		MaxPriority:        mp,
		GasLimit:           etx.EffectiveGasLimit(),
		Data:               etx.Data,
		AccessList:         etx.AccessList,
		ReadSlots:          readSlots,
		WriteSlots:         writeSlots,
		BalanceDelta:       balDelta,
		extraBalanceReads:  extraBalReads,
		extraBalanceWrites: extraBalWrites,
	}
}

// ToEVMTransaction converts a Scheduler Transaction into an evm.Transaction with derived access sets.
func (tx *Transaction) ToEVMTransaction() *evm.Transaction {
	if tx == nil {
		return nil
	}
	etx := &evm.Transaction{
		Hash:         tx.Hash,
		From:         tx.From,
		To:           tx.To,
		Nonce:        tx.Nonce,
		GasLimit:     tx.GasLimit,
		Data:         tx.Data,
		AccessList:   tx.AccessList,
		ReadSlots:    tx.ReadSlots,
		WriteSlots:   tx.WriteSlots,
		BalanceDelta: tx.BalanceDelta,
	}
	if tx.Value != nil {
		etx.Value = new(big.Int).Set(tx.Value)
	}
	if tx.GasPrice != nil {
		etx.GasPrice = new(big.Int).Set(tx.GasPrice)
	}
	if tx.MaxPriority != nil {
		etx.MaxPriority = new(big.Int).Set(tx.MaxPriority)
	}
	etx.DeriveStateAccess()
	return etx
}

func splitStorageKey(key string) (string, string) {
	idx := strings.IndexByte(key, ':')
	if idx < 0 {
		return key, ""
	}
	return key[:idx], key[idx+1:]
}

// Edge represents a directed dependency from an earlier transaction to a later one.
type Edge struct {
	FromTx string         `json:"from_tx"`
	ToTx   string         `json:"to_tx"`
	Type   DependencyType `json:"type"`
	Key    string         `json:"key,omitempty"`
	Detail string         `json:"detail"`
}

// ConflictReport summarizes state contention, hazards, and parallel efficiency across a mempool batch.
type ConflictReport struct {
	TotalTransactions  int                    `json:"total_transactions"`
	IndependentWaves   int                    `json:"independent_waves"`
	MaxConcurrency     int                    `json:"max_concurrency"`
	TheoreticalSpeedup float64                `json:"theoretical_speedup"`
	StateConflicts     int                    `json:"state_conflicts"`
	NonceDependencies  int                    `json:"nonce_dependencies"`
	BalanceHazards     int                    `json:"balance_hazards"`
	BalanceViolations  int                    `json:"balance_violations"`
	NonceConflicts     int                    `json:"nonce_conflicts"`
	NonceGaps          int                    `json:"nonce_gaps"`
	CyclesDetected     bool                   `json:"cycles_detected"`
	TotalGas           uint64                 `json:"total_gas"`
	CriticalPathLen    int                    `json:"critical_path_length"`
	CriticalPathGas    uint64                 `json:"critical_path_gas"`
	GasWeightedSpeedup float64                `json:"gas_weighted_speedup"`
	WaveGasSpeedup     float64                `json:"wave_gas_speedup"`
	ParallelEfficiency float64                `json:"parallel_efficiency"`
	HazardBreakdown    map[DependencyType]int `json:"hazard_breakdown,omitempty"`
}

// CriticalPath captures both hop-count and gas-weighted longest dependency chains in the DAG.
type CriticalPath struct {
	HopLength      int      `json:"hop_length"`
	HopChain       []string `json:"hop_chain"`
	GasWeight      uint64   `json:"gas_weight"`
	GasChain       []string `json:"gas_chain"`
	BottleneckKeys []string `json:"bottleneck_keys,omitempty"`
}

// WaveMetrics describes gas utilization and straggler load imbalance within a single execution wave.
type WaveMetrics struct {
	WaveIndex     int      `json:"wave_index"`
	TxCount       int      `json:"tx_count"`
	TotalGas      uint64   `json:"total_gas"`
	MaxTxGas      uint64   `json:"max_tx_gas"`
	AvgTxGas      float64  `json:"avg_tx_gas"`
	LoadImbalance float64  `json:"load_imbalance"`
	Transactions  []string `json:"transactions"`
}

// WorkerSimulation compares wave-barrier execution against work-stealing DAG dispatch
// for a fixed pool of parallel EVM execution workers.
type WorkerSimulation struct {
	Workers              int     `json:"workers"`
	SequentialGas        uint64  `json:"sequential_gas"`
	WaveBarrierGas       uint64  `json:"wave_barrier_gas"`
	WorkStealingGas      uint64  `json:"work_stealing_gas"`
	WaveBarrierSpeedup   float64 `json:"wave_barrier_speedup"`
	WorkStealingSpeedup  float64 `json:"work_stealing_speedup"`
	WaveUtilization      float64 `json:"wave_utilization"`
	WorkStealUtilization float64 `json:"work_steal_utilization"`
}

// ContentionHotspot identifies a storage slot or account driving serial execution bottlenecks.
type ContentionHotspot struct {
	Key        string   `json:"key"`
	WriteCount int      `json:"write_count"`
	ReadCount  int      `json:"read_count"`
	EdgeCount  int      `json:"edge_count"`
	TxHashes   []string `json:"tx_hashes"`
}

// NonceAnomaly reports sender nonce hazards such as duplicate replacement nonces or sequence gaps.
type NonceAnomaly struct {
	Sender        string   `json:"sender"`
	Kind          string   `json:"kind"` // "DUPLICATE_NONCE" or "NONCE_GAP"
	Nonce         uint64   `json:"nonce"`
	ExpectedNonce uint64   `json:"expected_nonce,omitempty"`
	TxHashes      []string `json:"tx_hashes"`
}

// BalanceViolation flags a transaction whose debits exceed the tracked account balance.
type BalanceViolation struct {
	Account  string `json:"account"`
	TxHash   string `json:"tx_hash"`
	Required string `json:"required"`
	Balance  string `json:"balance"`
}

// ScheduleAnalysis aggregates the complete DAG scheduling diagnostics for a batch.
type ScheduleAnalysis struct {
	Report            *ConflictReport     `json:"report"`
	CriticalPath      CriticalPath        `json:"critical_path"`
	WaveMetrics       []WaveMetrics       `json:"wave_metrics"`
	WorkerSimulation  WorkerSimulation    `json:"worker_simulation"`
	Hotspots          []ContentionHotspot `json:"hotspots,omitempty"`
	NonceAnomalies    []NonceAnomaly      `json:"nonce_anomalies,omitempty"`
	BalanceViolations []BalanceViolation  `json:"balance_violations,omitempty"`
}
