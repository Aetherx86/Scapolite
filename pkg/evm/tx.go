package evm

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"
)

// Baseline EVM gas estimates used when a transaction omits an explicit GasLimit.
const (
	DefaultTransferGas       uint64 = 21_000
	DefaultERC20Gas          uint64 = 65_000
	DefaultContractCallGas   uint64 = 120_000
	DefaultAMMSwapGas        uint64 = 150_000
	DefaultContractCreateGas uint64 = 210_000
)

// AccessMode indicates whether a state slot is read, written, or both.
type AccessMode uint8

const (
	AccessRead AccessMode = 1 << iota
	AccessWrite
	AccessReadWrite = AccessRead | AccessWrite
)

// String renders the access mode in compact R/W/RW notation.
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

// StorageKey identifies a specific 32-byte storage slot or logical state bucket within a contract.
type StorageKey struct {
	Address string // 20-byte hex address (0x...)
	Slot    string // 32-byte hex storage slot or canonical logical key
}

// String returns the canonical lowercase "address:slot" representation.
func (k StorageKey) String() string {
	return fmt.Sprintf("%s:%s", normalizeHex(k.Address), strings.ToLower(k.Slot))
}

// NonceKey returns the canonical account nonce key for an address.
func NonceKey(addr string) string {
	return "nonce:" + normalizeHex(addr)
}

// BalanceKey returns the canonical native ETH balance key for an address.
func BalanceKey(addr string) string {
	return "balance:" + normalizeHex(addr)
}

// CreateKey returns the canonical contract creation key for a sender and nonce.
func CreateKey(from string, nonce uint64) string {
	return fmt.Sprintf("create:%s:%d", normalizeHex(from), nonce)
}

// ContractKey returns the canonical coarse contract lock key for an address.
func ContractKey(addr string) string {
	return "contract:" + normalizeHex(addr)
}

// IsBalanceKey reports whether key represents a native account balance state entry.
func IsBalanceKey(key string) bool {
	return strings.HasPrefix(key, "balance:")
}

// StateAccess records an operation against a storage key or account state.
type StateAccess struct {
	Key  StorageKey
	Mode AccessMode
}

// AccessTuple represents an EIP-2930 / EIP-1559 access list entry.
type AccessTuple struct {
	Address     string   `json:"address"`
	StorageKeys []string `json:"storageKeys"`
}

// Transaction models an EVM transaction together with its explicit and derived state footprint.
type Transaction struct {
	Hash         string              `json:"hash"`
	From         string              `json:"from"`
	To           string              `json:"to"`
	Nonce        uint64              `json:"nonce"`
	Value        *big.Int            `json:"value,omitempty"`
	GasPrice     *big.Int            `json:"gasPrice,omitempty"`
	MaxPriority  *big.Int            `json:"maxPriorityFeePerGas,omitempty"`
	GasLimit     uint64              `json:"gasLimit,omitempty"`
	Data         []byte              `json:"data,omitempty"`
	AccessList   []AccessTuple       `json:"accessList,omitempty"`
	ReadSlots    map[string][]string `json:"read_slots,omitempty"`
	WriteSlots   map[string][]string `json:"write_slots,omitempty"`
	BalanceDelta map[string]*big.Int `json:"balance_deltas,omitempty"`

	// Derived state access footprints populated by DeriveStateAccess.
	ReadSet       map[string]struct{} `json:"-"`
	WriteSet      map[string]struct{} `json:"-"`
	BalanceReads  map[string]struct{} `json:"-"`
	BalanceWrites map[string]struct{} `json:"-"`
	Selector      string              `json:"-"`
	Action        string              `json:"-"`
}

// NewTransaction initializes a transaction and computes its derived read/write access footprint.
func NewTransaction(hash, from, to string, nonce uint64, gasPrice, maxPriority *big.Int, data []byte, accessList []AccessTuple) *Transaction {
	return NewValueTransaction(hash, from, to, nonce, nil, gasPrice, maxPriority, 0, data, accessList)
}

// NewValueTransaction initializes a transaction with explicit ETH value and gas limit.
func NewValueTransaction(hash, from, to string, nonce uint64, value, gasPrice, maxPriority *big.Int, gasLimit uint64, data []byte, accessList []AccessTuple) *Transaction {
	normFrom := normalizeHex(from)
	normTo := normalizeHex(to)
	if hash == "" {
		valStr := "0"
		if value != nil {
			valStr = value.String()
		}
		h := sha256.Sum256([]byte(fmt.Sprintf("%s:%s:%d:%s:%x", normFrom, normTo, nonce, valStr, data)))
		hash = "0x" + hex.EncodeToString(h[:])
	}

	tx := &Transaction{
		Hash:        normalizeHex(hash),
		From:        normFrom,
		To:          normTo,
		Nonce:       nonce,
		Value:       cloneBigInt(value),
		GasPrice:    cloneBigInt(gasPrice),
		MaxPriority: cloneBigInt(maxPriority),
		GasLimit:    gasLimit,
		Data:        cloneBytes(data),
		AccessList:  cloneAccessList(accessList),
	}

	tx.DeriveStateAccess()
	return tx
}

// DeriveStateAccess recomputes canonical storage and balance access sets from
// explicit footprints, EIP-2930 access lists, value transfers, and ABI calldata.
func (tx *Transaction) DeriveStateAccess() {
	tx.Hash = normalizeHex(tx.Hash)
	tx.From = normalizeHex(tx.From)
	tx.To = normalizeHex(tx.To)
	if tx.Hash == "" {
		valStr := "0"
		if tx.Value != nil {
			valStr = tx.Value.String()
		}
		h := sha256.Sum256([]byte(fmt.Sprintf("%s:%s:%d:%s:%x", tx.From, tx.To, tx.Nonce, valStr, tx.Data)))
		tx.Hash = "0x" + hex.EncodeToString(h[:])
	}

	tx.ReadSet = make(map[string]struct{})
	tx.WriteSet = make(map[string]struct{})
	tx.BalanceReads = make(map[string]struct{})
	tx.BalanceWrites = make(map[string]struct{})
	tx.Selector = ""
	tx.Action = "eth_transfer"

	// 1. Sender native balance accounting:
	// Paying gas or transferring non-zero ETH reads and debits tx.From's balance.
	hasValue := tx.Value != nil && tx.Value.Sign() > 0
	hasGasCost := tx.GasPrice != nil && tx.GasPrice.Sign() > 0
	if tx.From != "" && (hasValue || hasGasCost) {
		tx.markBalanceReadWrite(tx.From)
	}

	// 2. Contract creation vs native ETH value credit:
	// Only value-bearing calls mutate the recipient's native ETH balance; zero-value
	// contract invocations leave the target contract's native ETH balance untouched,
	// allowing fine-grained storage slot parallelism across calls to the same contract.
	if tx.To == "" {
		tx.Action = "contract_create"
		if tx.From != "" {
			tx.WriteSet[CreateKey(tx.From, tx.Nonce)] = struct{}{}
		}
	} else if hasValue {
		tx.markBalanceWrite(tx.To)
	}

	// 3. Explicit storage slot footprints (ReadSlots / WriteSlots).
	hasExplicitSlots := len(tx.ReadSlots) > 0 || len(tx.WriteSlots) > 0
	for contract, slots := range tx.ReadSlots {
		addr := normalizeHex(contract)
		for _, slot := range slots {
			key := StorageKey{Address: addr, Slot: slot}.String()
			tx.ReadSet[key] = struct{}{}
		}
	}
	for contract, slots := range tx.WriteSlots {
		addr := normalizeHex(contract)
		for _, slot := range slots {
			key := StorageKey{Address: addr, Slot: slot}.String()
			tx.WriteSet[key] = struct{}{}
		}
	}

	// 4. Explicit balance deltas.
	for acct, delta := range tx.BalanceDelta {
		addr := normalizeHex(acct)
		if addr == "" || delta == nil || delta.Sign() == 0 {
			continue
		}
		if delta.Sign() < 0 {
			tx.markBalanceReadWrite(addr)
		} else {
			tx.markBalanceWrite(addr)
		}
	}

	// 5. EIP-2930 / EIP-1559 AccessList warming.
	// EIP-2930 does not distinguish read-only vs read-write slots, so declared
	// storage keys are conservatively marked RW unless explicit slots already cover them.
	for _, tuple := range tx.AccessList {
		addr := normalizeHex(tuple.Address)
		if addr == "" {
			continue
		}
		if len(tuple.StorageKeys) == 0 {
			tx.ReadSet[StorageKey{Address: addr, Slot: "account_meta"}.String()] = struct{}{}
			continue
		}
		for _, slot := range tuple.StorageKeys {
			key := StorageKey{Address: addr, Slot: slot}.String()
			tx.ReadSet[key] = struct{}{}
			tx.WriteSet[key] = struct{}{}
		}
	}

	// 6. ABI calldata method selector heuristics for common DeFi / token protocols.
	if len(tx.Data) >= 4 && tx.To != "" {
		selector := hex.EncodeToString(tx.Data[:4])
		tx.Selector = "0x" + selector

		switch selector {
		case "a9059cbb": // ERC-20 transfer(address to, uint256 amount)
			tx.Action = "erc20_transfer"
			if len(tx.Data) >= 36 {
				recipient := "0x" + hex.EncodeToString(tx.Data[16:36])
				senderSlot := StorageKey{Address: tx.To, Slot: "erc20:bal:" + tx.From}.String()
				recipSlot := StorageKey{Address: tx.To, Slot: "erc20:bal:" + recipient}.String()
				tx.ReadSet[senderSlot] = struct{}{}
				tx.WriteSet[senderSlot] = struct{}{}
				tx.ReadSet[recipSlot] = struct{}{}
				tx.WriteSet[recipSlot] = struct{}{}
			} else if !hasExplicitSlots && len(tx.AccessList) == 0 {
				tx.markOpaqueContract(tx.To)
			}

		case "23b872dd": // ERC-20 transferFrom(address from, address to, uint256 amount)
			tx.Action = "erc20_transfer_from"
			if len(tx.Data) >= 68 {
				src := "0x" + hex.EncodeToString(tx.Data[16:36])
				dst := "0x" + hex.EncodeToString(tx.Data[48:68])
				srcSlot := StorageKey{Address: tx.To, Slot: "erc20:bal:" + src}.String()
				dstSlot := StorageKey{Address: tx.To, Slot: "erc20:bal:" + dst}.String()
				allowSlot := StorageKey{Address: tx.To, Slot: "erc20:allow:" + src + ":" + tx.From}.String()
				tx.ReadSet[srcSlot] = struct{}{}
				tx.WriteSet[srcSlot] = struct{}{}
				tx.ReadSet[dstSlot] = struct{}{}
				tx.WriteSet[dstSlot] = struct{}{}
				tx.ReadSet[allowSlot] = struct{}{}
				tx.WriteSet[allowSlot] = struct{}{}
			} else if !hasExplicitSlots && len(tx.AccessList) == 0 {
				tx.markOpaqueContract(tx.To)
			}

		case "095ea7b3": // ERC-20 approve(address spender, uint256 amount)
			tx.Action = "erc20_approve"
			if len(tx.Data) >= 36 {
				spender := "0x" + hex.EncodeToString(tx.Data[16:36])
				allowSlot := StorageKey{Address: tx.To, Slot: "erc20:allow:" + tx.From + ":" + spender}.String()
				tx.WriteSet[allowSlot] = struct{}{}
			} else if !hasExplicitSlots && len(tx.AccessList) == 0 {
				tx.markOpaqueContract(tx.To)
			}

		case "38ed1739", // Uniswap V2 swapExactTokensForTokens
			"7ff36ab5", // Uniswap V2 swapExactETHForTokens
			"18cbafe5", // Uniswap V2 swapExactTokensForETH
			"022c0d48", // Uniswap V2 Pair swap
			"414bf389", // Uniswap V3 exactInputSingle
			"c04b8d59": // Uniswap V3 exactInput
			tx.Action = "amm_swap"
			poolSlot := StorageKey{Address: tx.To, Slot: "amm:reserve_or_tick"}.String()
			tx.ReadSet[poolSlot] = struct{}{}
			tx.WriteSet[poolSlot] = struct{}{}
			if selector == "7ff36ab5" || selector == "18cbafe5" {
				if tx.From != "" {
					tx.markBalanceReadWrite(tx.From)
				}
				tx.markBalanceReadWrite(tx.To)
			}

		case "e8e33700", // Uniswap V2 addLiquidity
			"f305d719", // Uniswap V2 addLiquidityETH
			"baa2abde": // Uniswap V2 removeLiquidity
			tx.Action = "amm_liquidity"
			poolSlot := StorageKey{Address: tx.To, Slot: "amm:reserve_or_tick"}.String()
			lpSlot := StorageKey{Address: tx.To, Slot: "amm:lp:" + tx.From}.String()
			tx.ReadSet[poolSlot] = struct{}{}
			tx.WriteSet[poolSlot] = struct{}{}
			tx.ReadSet[lpSlot] = struct{}{}
			tx.WriteSet[lpSlot] = struct{}{}

		case "d0e30db0": // WETH deposit()
			tx.Action = "weth_wrap"
			balSlot := StorageKey{Address: tx.To, Slot: "erc20:bal:" + tx.From}.String()
			tx.ReadSet[balSlot] = struct{}{}
			tx.WriteSet[balSlot] = struct{}{}
			if tx.From != "" {
				tx.markBalanceReadWrite(tx.From)
			}
			tx.markBalanceReadWrite(tx.To)

		case "2e1a7d4d": // WETH withdraw(uint256)
			tx.Action = "weth_unwrap"
			balSlot := StorageKey{Address: tx.To, Slot: "erc20:bal:" + tx.From}.String()
			tx.ReadSet[balSlot] = struct{}{}
			tx.WriteSet[balSlot] = struct{}{}
			if tx.From != "" {
				tx.markBalanceReadWrite(tx.From)
			}
			tx.markBalanceReadWrite(tx.To)

		default:
			tx.Action = "contract_call"
			// Without explicit storage slots or an EIP-2930 access list, conservatively
			// serialize unknown contract mutations against the target contract.
			if !hasExplicitSlots && len(tx.AccessList) == 0 {
				tx.markOpaqueContract(tx.To)
			}
		}
	} else if tx.To != "" && hasExplicitSlots {
		tx.Action = "contract_call"
	}
}

func (tx *Transaction) markBalanceWrite(addr string) {
	addr = normalizeHex(addr)
	if addr == "" {
		return
	}
	tx.BalanceWrites[addr] = struct{}{}
	tx.WriteSet[BalanceKey(addr)] = struct{}{}
}

func (tx *Transaction) markBalanceReadWrite(addr string) {
	addr = normalizeHex(addr)
	if addr == "" {
		return
	}
	tx.BalanceReads[addr] = struct{}{}
	tx.BalanceWrites[addr] = struct{}{}
	tx.ReadSet[BalanceKey(addr)] = struct{}{}
	tx.WriteSet[BalanceKey(addr)] = struct{}{}
}

func (tx *Transaction) markOpaqueContract(addr string) {
	key := StorageKey{Address: addr, Slot: "contract:opaque_state"}.String()
	tx.ReadSet[key] = struct{}{}
	tx.WriteSet[key] = struct{}{}
}

// EffectiveGasTip returns the EIP-1559 priority fee if positive, falling back to legacy gasPrice.
func (tx *Transaction) EffectiveGasTip() *big.Int {
	if tx.MaxPriority != nil && tx.MaxPriority.Sign() > 0 {
		return new(big.Int).Set(tx.MaxPriority)
	}
	if tx.GasPrice != nil && tx.GasPrice.Sign() > 0 {
		return new(big.Int).Set(tx.GasPrice)
	}
	return big.NewInt(0)
}

// EffectiveGasLimit returns the explicit GasLimit if set, or a deterministic baseline
// estimate inferred from the decoded EVM transaction action.
func (tx *Transaction) EffectiveGasLimit() uint64 {
	if tx.GasLimit > 0 {
		return tx.GasLimit
	}
	switch tx.Action {
	case "contract_create":
		return DefaultContractCreateGas
	case "amm_swap", "amm_liquidity":
		return DefaultAMMSwapGas
	case "erc20_transfer", "erc20_transfer_from", "erc20_approve", "weth_wrap", "weth_unwrap":
		return DefaultERC20Gas
	case "contract_call":
		return DefaultContractCallGas
	default:
		return DefaultTransferGas
	}
}

// MaxGasCost computes GasLimit * GasPrice (using EffectiveGasLimit when GasLimit is 0).
func (tx *Transaction) MaxGasCost() *big.Int {
	if tx.GasPrice == nil || tx.GasPrice.Sign() <= 0 {
		return big.NewInt(0)
	}
	gas := new(big.Int).SetUint64(tx.EffectiveGasLimit())
	return gas.Mul(gas, tx.GasPrice)
}

// EstimatedTotalDebit computes the maximum native ETH debited from tx.From (Value + MaxGasCost).
func (tx *Transaction) EstimatedTotalDebit() *big.Int {
	total := tx.MaxGasCost()
	if tx.Value != nil && tx.Value.Sign() > 0 {
		total.Add(total, tx.Value)
	}
	return total
}

// EncodeERC20Transfer builds ABI-encoded calldata for transfer(address,uint256).
func EncodeERC20Transfer(recipient string, amount *big.Int) []byte {
	out := make([]byte, 4+32+32)
	out[0], out[1], out[2], out[3] = 0xa9, 0x05, 0x9c, 0xbb
	copyAddressWord(out[4:36], recipient)
	copyUint256Word(out[36:68], amount)
	return out
}

// EncodeERC20Approve builds ABI-encoded calldata for approve(address,uint256).
func EncodeERC20Approve(spender string, amount *big.Int) []byte {
	out := make([]byte, 4+32+32)
	out[0], out[1], out[2], out[3] = 0x09, 0x5e, 0xa7, 0xb3
	copyAddressWord(out[4:36], spender)
	copyUint256Word(out[36:68], amount)
	return out
}

// EncodeERC20TransferFrom builds ABI-encoded calldata for transferFrom(address,address,uint256).
func EncodeERC20TransferFrom(src, dst string, amount *big.Int) []byte {
	out := make([]byte, 4+32+32+32)
	out[0], out[1], out[2], out[3] = 0x23, 0xb8, 0x72, 0xdd
	copyAddressWord(out[4:36], src)
	copyAddressWord(out[36:68], dst)
	copyUint256Word(out[68:100], amount)
	return out
}

// EncodeUniswapSwap builds ABI-encoded calldata with the Uniswap V2 swapExactTokensForTokens selector.
func EncodeUniswapSwap(amountIn *big.Int) []byte {
	out := make([]byte, 4+32)
	out[0], out[1], out[2], out[3] = 0x38, 0xed, 0x17, 0x39
	copyUint256Word(out[4:36], amountIn)
	return out
}

func copyAddressWord(dst []byte, addr string) {
	cleaned := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(addr)), "0x")
	if len(cleaned) > 40 {
		cleaned = cleaned[len(cleaned)-40:]
	}
	if len(cleaned)%2 == 1 {
		cleaned = "0" + cleaned
	}
	raw, err := hex.DecodeString(cleaned)
	if err != nil || len(raw) == 0 {
		return
	}
	if len(raw) > 20 {
		raw = raw[len(raw)-20:]
	}
	copy(dst[32-len(raw):], raw)
}

func copyUint256Word(dst []byte, val *big.Int) {
	if val == nil || val.Sign() <= 0 {
		return
	}
	b := val.Bytes()
	if len(b) > 32 {
		b = b[len(b)-32:]
	}
	copy(dst[32-len(b):], b)
}

func normalizeHex(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

func cloneBigInt(v *big.Int) *big.Int {
	if v == nil {
		return nil
	}
	return new(big.Int).Set(v)
}

func cloneBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	cp := make([]byte, len(b))
	copy(cp, b)
	return cp
}

func cloneAccessList(list []AccessTuple) []AccessTuple {
	if len(list) == 0 {
		return nil
	}
	cp := make([]AccessTuple, len(list))
	for i, tuple := range list {
		keys := make([]string, len(tuple.StorageKeys))
		copy(keys, tuple.StorageKeys)
		cp[i] = AccessTuple{
			Address:     tuple.Address,
			StorageKeys: keys,
		}
	}
	return cp
}
