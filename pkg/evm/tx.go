package evm

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"
)

// AccessMode indicates whether a state slot is read or written.
type AccessMode uint8

const (
	AccessRead AccessMode = 1 << iota
	AccessWrite
	AccessReadWrite = AccessRead | AccessWrite
)

func (m AccessMode) String() string {
	switch m {
	case AccessRead:
		return "R"
	case AccessWrite:
		return "W"
	case AccessReadWrite:
		return "RW"
	default:
		return "?"
	}
}

// StorageKey identifies a specific 32-byte storage slot within a contract address.
type StorageKey struct {
	Address string // 20-byte hex address (0x...)
	Slot    string // 32-byte hex storage slot (0x...)
}

func (k StorageKey) String() string {
	return fmt.Sprintf("%s:%s", strings.ToLower(k.Address), strings.ToLower(k.Slot))
}

// StateAccess records an operation against a storage key or account state.
type StateAccess struct {
	Key  StorageKey
	Mode AccessMode
}

// AccessTuple represents an EIP-2930 access list element.
type AccessTuple struct {
	Address     string   `json:"address"`
	StorageKeys []string `json:"storageKeys"`
}

// Transaction represents an EVM transaction with its state access footprint.
type Transaction struct {
	Hash        string        `json:"hash"`
	From        string        `json:"from"`
	To          string        `json:"to"`
	Nonce       uint64        `json:"nonce"`
	Value       *big.Int      `json:"value,omitempty"`
	GasPrice    *big.Int      `json:"gasPrice,omitempty"`
	MaxPriority *big.Int      `json:"maxPriorityFeePerGas,omitempty"`
	GasLimit    uint64        `json:"gasLimit"`
	Data        []byte        `json:"data,omitempty"`
	AccessList  []AccessTuple `json:"accessList,omitempty"`

	// Derived state access footprint
	ReadSet  map[string]struct{} `json:"-"`
	WriteSet map[string]struct{} `json:"-"`
}

// NewTransaction initializes a transaction and computes its read/write access sets.
func NewTransaction(hash, from, to string, nonce uint64, gasPrice, maxPriority *big.Int, data []byte, accessList []AccessTuple) *Transaction {
	if hash == "" {
		h := sha256.Sum256([]byte(fmt.Sprintf("%s:%s:%d:%x", from, to, nonce, data)))
		hash = "0x" + hex.EncodeToString(h[:])
	}

	tx := &Transaction{
		Hash:        strings.ToLower(hash),
		From:        strings.ToLower(from),
		To:          strings.ToLower(to),
		Nonce:       nonce,
		GasPrice:    gasPrice,
		MaxPriority: maxPriority,
		Data:        data,
		AccessList:  accessList,
		ReadSet:     make(map[string]struct{}),
		WriteSet:    make(map[string]struct{}),
	}

	tx.deriveStateAccess()
	return tx
}

// deriveStateAccess extracts implicit and explicit storage slots touched by the transaction.
func (tx *Transaction) deriveStateAccess() {
	// 1. Sender account state (nonce is incremented, balance deducted for gas/value -> Write)
	fromNonceKey := StorageKey{Address: tx.From, Slot: "0x0000000000000000000000000000000000000000000000000000000000000000"}.String()
	tx.WriteSet[fromNonceKey] = struct{}{}

	// 2. Recipient account balance (balance credited -> Write)
	if tx.To != "" {
		toBalKey := StorageKey{Address: tx.To, Slot: "0x0000000000000000000000000000000000000000000000000000000000000001"}.String()
		tx.WriteSet[toBalKey] = struct{}{}
	}

	// 3. Explicit EIP-2930 / EIP-1559 AccessList
	for _, tuple := range tx.AccessList {
		addr := strings.ToLower(tuple.Address)
		// Account access
		tx.ReadSet[StorageKey{Address: addr, Slot: "0x00"}.String()] = struct{}{}
		for _, slot := range tuple.StorageKeys {
			key := StorageKey{Address: addr, Slot: strings.ToLower(slot)}.String()
			tx.WriteSet[key] = struct{}{}
		}
	}

	// 4. Method selector heuristic for standard DeFi protocols (Uniswap, ERC20)
	if len(tx.Data) >= 4 {
		selector := hex.EncodeToString(tx.Data[:4])
		switch selector {
		case "a9059cbb": // transfer(address,uint256)
			if len(tx.Data) >= 36 && tx.To != "" {
				recipient := "0x" + hex.EncodeToString(tx.Data[16:36])
				balKey := StorageKey{Address: tx.To, Slot: "erc20:bal:" + recipient}.String()
				senderBalKey := StorageKey{Address: tx.To, Slot: "erc20:bal:" + tx.From}.String()
				tx.ReadSet[senderBalKey] = struct{}{}
				tx.WriteSet[senderBalKey] = struct{}{}
				tx.WriteSet[balKey] = struct{}{}
			}
		case "095ea7b3": // approve(address,uint256)
			if len(tx.Data) >= 36 && tx.To != "" {
				spender := "0x" + hex.EncodeToString(tx.Data[16:36])
				allowanceKey := StorageKey{Address: tx.To, Slot: "erc20:allow:" + tx.From + ":" + spender}.String()
				tx.WriteSet[allowanceKey] = struct{}{}
			}
		case "38ed1739", "7ff36ab5", "18cbafe5", "022c0d48": // Uniswap V2 / V3 swap functions
			// Swaps write to the pool reserves / liquidity slot
			poolKey := StorageKey{Address: tx.To, Slot: "amm:reserve_or_tick"}.String()
			tx.ReadSet[poolKey] = struct{}{}
			tx.WriteSet[poolKey] = struct{}{}
		}
	}
}

// EffectiveGasTip returns the priority fee or gas price for sorting and MEV gradient evaluation.
func (tx *Transaction) EffectiveGasTip() *big.Int {
	if tx.MaxPriority != nil && tx.MaxPriority.Sign() > 0 {
		return tx.MaxPriority
	}
	if tx.GasPrice != nil {
		return tx.GasPrice
	}
	return big.NewInt(0)
}
