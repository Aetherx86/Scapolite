package dag

import (
	"math/big"
)

type DependencyType string

const (
	DepNonce        DependencyType = "NONCE_ORDER"
	DepReadWrite    DependencyType = "READ_AFTER_WRITE"
	DepWriteRead    DependencyType = "WRITE_AFTER_READ"
	DepWriteWrite   DependencyType = "WRITE_AFTER_WRITE"
	DepBalanceDrain DependencyType = "BALANCE_VIOLATION"
)

// Transaction models an EVM transaction with expected state access footprints.
type Transaction struct {
	Hash         string            `json:"hash"`
	From         string            `json:"from"`
	To           string            `json:"to"`
	Nonce        uint64            `json:"nonce"`
	Value        *big.Int          `json:"value"`
	GasPrice     *big.Int          `json:"gas_price"`
	GasLimit     uint64            `json:"gas_limit"`
	Data         []byte            `json:"-"`
	ReadSlots    map[string][]string `json:"read_slots,omitempty"`  // contractAddr -> []storageKey
	WriteSlots   map[string][]string `json:"write_slots,omitempty"` // contractAddr -> []storageKey
	BalanceDelta map[string]*big.Int `json:"balance_deltas,omitempty"`
}

// Edge represents a directed dependency from an earlier transaction to a later one.
type Edge struct {
	FromTx string         `json:"from_tx"`
	ToTx   string         `json:"to_tx"`
	Type   DependencyType `json:"type"`
	Detail string         `json:"detail"`
}

// ConflictReport summarizes state contention across a mempool transaction batch.
type ConflictReport struct {
	TotalTransactions int      `json:"total_transactions"`
	IndependentWaves  int      `json:"independent_waves"`
	MaxConcurrency    int      `json:"max_concurrency"`
	TheoreticalSpeedup float64 `json:"theoretical_speedup"`
	StateConflicts    int      `json:"state_conflicts"`
	NonceDependencies int      `json:"nonce_dependencies"`
	CyclesDetected    bool     `json:"cycles_detected"`
}
