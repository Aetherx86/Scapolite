package dag

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"math/rand"
	"os"
	"strings"

	"github.com/Aetherx86/Scapolite/pkg/evm"
)

// IngestMempoolJSON parses an array of scheduler transactions from a reader or file.
func IngestMempoolJSON(r io.Reader) ([]*Transaction, error) {
	var txs []*Transaction
	dec := json.NewDecoder(r)
	if err := dec.Decode(&txs); err != nil {
		return nil, fmt.Errorf("failed to decode mempool JSON: %w", err)
	}
	return txs, nil
}

// rawEVMTransaction accepts both Ethereum/EIP-2930 camelCase JSON and Scapolite snake_case JSON.
type rawEVMTransaction struct {
	Hash             string                     `json:"hash"`
	From             string                     `json:"from"`
	To               string                     `json:"to"`
	Nonce            uint64                     `json:"nonce"`
	Value            json.RawMessage            `json:"value,omitempty"`
	GasPrice         json.RawMessage            `json:"gasPrice,omitempty"`
	GasPriceSnake    json.RawMessage            `json:"gas_price,omitempty"`
	MaxPriority      json.RawMessage            `json:"maxPriorityFeePerGas,omitempty"`
	MaxPrioritySnake json.RawMessage            `json:"max_priority_fee_per_gas,omitempty"`
	GasLimit         uint64                     `json:"gasLimit,omitempty"`
	GasLimitSnake    uint64                     `json:"gas_limit,omitempty"`
	Data             json.RawMessage            `json:"data,omitempty"`
	AccessList       []evm.AccessTuple          `json:"accessList,omitempty"`
	AccessListSnake  []evm.AccessTuple          `json:"access_list,omitempty"`
	ReadSlots        map[string][]string        `json:"read_slots,omitempty"`
	WriteSlots       map[string][]string        `json:"write_slots,omitempty"`
	BalanceDeltas    map[string]json.RawMessage `json:"balance_deltas,omitempty"`
}

// IngestEVMMempoolJSON parses a JSON array of EVM transactions in either Ethereum RPC /
// EIP-2930 format or Scapolite slot-footprint format and derives each transaction's access sets.
func IngestEVMMempoolJSON(r io.Reader) ([]*evm.Transaction, error) {
	var rawList []rawEVMTransaction
	dec := json.NewDecoder(r)
	if err := dec.Decode(&rawList); err != nil {
		return nil, fmt.Errorf("failed to decode EVM mempool JSON: %w", err)
	}

	txs := make([]*evm.Transaction, 0, len(rawList))
	for i, raw := range rawList {
		val, err := parseBigIntFlex(raw.Value)
		if err != nil {
			return nil, fmt.Errorf("tx[%d] invalid value: %w", i, err)
		}

		gpRaw := raw.GasPrice
		if len(gpRaw) == 0 {
			gpRaw = raw.GasPriceSnake
		}
		gp, err := parseBigIntFlex(gpRaw)
		if err != nil {
			return nil, fmt.Errorf("tx[%d] invalid gasPrice: %w", i, err)
		}

		mpRaw := raw.MaxPriority
		if len(mpRaw) == 0 {
			mpRaw = raw.MaxPrioritySnake
		}
		mp, err := parseBigIntFlex(mpRaw)
		if err != nil {
			return nil, fmt.Errorf("tx[%d] invalid maxPriorityFeePerGas: %w", i, err)
		}

		gasLimit := raw.GasLimit
		if gasLimit == 0 {
			gasLimit = raw.GasLimitSnake
		}

		dataBytes, err := parseHexBytesFlex(raw.Data)
		if err != nil {
			return nil, fmt.Errorf("tx[%d] invalid data: %w", i, err)
		}

		accessList := raw.AccessList
		if len(accessList) == 0 {
			accessList = raw.AccessListSnake
		}

		var deltas map[string]*big.Int
		if len(raw.BalanceDeltas) > 0 {
			deltas = make(map[string]*big.Int, len(raw.BalanceDeltas))
			for acct, rawDelta := range raw.BalanceDeltas {
				d, err := parseBigIntFlex(rawDelta)
				if err != nil {
					return nil, fmt.Errorf("tx[%d] invalid balance_deltas[%s]: %w", i, acct, err)
				}
				if d != nil {
					deltas[acct] = d
				}
			}
		}

		tx := &evm.Transaction{
			Hash:         raw.Hash,
			From:         raw.From,
			To:           raw.To,
			Nonce:        raw.Nonce,
			Value:        val,
			GasPrice:     gp,
			MaxPriority:  mp,
			GasLimit:     gasLimit,
			Data:         dataBytes,
			AccessList:   accessList,
			ReadSlots:    raw.ReadSlots,
			WriteSlots:   raw.WriteSlots,
			BalanceDelta: deltas,
		}
		tx.DeriveStateAccess()
		txs = append(txs, tx)
	}

	return txs, nil
}

// evmTransactionExport mirrors Ethereum JSON serialization with 0x-prefixed hex calldata.
type evmTransactionExport struct {
	Hash         string              `json:"hash"`
	From         string              `json:"from"`
	To           string              `json:"to"`
	Nonce        uint64              `json:"nonce"`
	Value        string              `json:"value,omitempty"`
	GasPrice     string              `json:"gasPrice,omitempty"`
	MaxPriority  string              `json:"maxPriorityFeePerGas,omitempty"`
	GasLimit     uint64              `json:"gasLimit,omitempty"`
	Data         string              `json:"data,omitempty"`
	AccessList   []evm.AccessTuple   `json:"accessList,omitempty"`
	ReadSlots    map[string][]string `json:"read_slots,omitempty"`
	WriteSlots   map[string][]string `json:"write_slots,omitempty"`
	BalanceDelta map[string]string   `json:"balance_deltas,omitempty"`
}

// DumpEVMMempoolJSON serializes EVM transactions to JSON with standard 0x-hex calldata.
func DumpEVMMempoolJSON(w io.Writer, txs []*evm.Transaction) error {
	exported := make([]evmTransactionExport, len(txs))
	for i, tx := range txs {
		item := evmTransactionExport{
			Hash:       tx.Hash,
			From:       tx.From,
			To:         tx.To,
			Nonce:      tx.Nonce,
			GasLimit:   tx.EffectiveGasLimit(),
			AccessList: tx.AccessList,
			ReadSlots:  tx.ReadSlots,
			WriteSlots: tx.WriteSlots,
		}
		if tx.Value != nil {
			item.Value = tx.Value.String()
		}
		if tx.GasPrice != nil {
			item.GasPrice = tx.GasPrice.String()
		}
		if tx.MaxPriority != nil {
			item.MaxPriority = tx.MaxPriority.String()
		}
		if len(tx.Data) > 0 {
			item.Data = "0x" + hex.EncodeToString(tx.Data)
		}
		if len(tx.BalanceDelta) > 0 {
			item.BalanceDelta = make(map[string]string, len(tx.BalanceDelta))
			for k, v := range tx.BalanceDelta {
				if v != nil {
					item.BalanceDelta[k] = v.String()
				}
			}
		}
		exported[i] = item
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(exported)
}

// ExportEVMMempoolFile writes an EVM mempool batch to disk as JSON.
func ExportEVMMempoolFile(path string, txs []*evm.Transaction) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return DumpEVMMempoolJSON(f, txs)
}

// GenerateSyntheticMempool creates a deterministic batch of scheduler transactions for benchmarking.
func GenerateSyntheticMempool(count int, seed int64) []*Transaction {
	if count <= 0 {
		return nil
	}
	rng := rand.New(rand.NewSource(seed))
	txs := make([]*Transaction, count)

	numAccounts := count / 5
	if numAccounts < 2 {
		numAccounts = 2
	}
	accounts := make([]string, numAccounts)
	for i := 0; i < numAccounts; i++ {
		accounts[i] = fmt.Sprintf("0x%040x", i+1)
	}

	numContracts := count / 10
	if numContracts < 1 {
		numContracts = 1
	}
	contracts := make([]string, numContracts)
	for i := 0; i < numContracts; i++ {
		contracts[i] = fmt.Sprintf("0x%040x", 0x1000+i)
	}

	accountNonces := make(map[string]uint64)

	for i := 0; i < count; i++ {
		from := accounts[rng.Intn(numAccounts)]
		nonce := accountNonces[from]
		accountNonces[from]++

		to := contracts[rng.Intn(numContracts)]

		gasPrice := big.NewInt(int64(20 + rng.Intn(100)))
		gasPrice.Mul(gasPrice, big.NewInt(1_000_000_000)) // Gwei

		hBytes := sha256.Sum256([]byte(fmt.Sprintf("%s:%d:%d", from, nonce, i)))
		txHash := "0x" + hex.EncodeToString(hBytes[:])

		readSlots := make(map[string][]string)
		writeSlots := make(map[string][]string)
		gasLimit := uint64(21_000)

		// 30% chance of accessing shared contract storage
		if rng.Float64() < 0.3 {
			slotID := fmt.Sprintf("slot_%d", rng.Intn(5))
			gasLimit = uint64(65_000 + rng.Intn(115_000))
			if rng.Float64() < 0.5 {
				writeSlots[to] = []string{slotID}
			} else {
				readSlots[to] = []string{slotID}
			}
		}

		// Native ETH value only on plain value transfers (contract calls carry zero value)
		val := big.NewInt(0)
		if len(readSlots) == 0 && len(writeSlots) == 0 && rng.Float64() < 0.4 {
			val = big.NewInt(int64(1 + rng.Intn(10)))
		}

		txs[i] = &Transaction{
			Hash:       txHash,
			From:       from,
			To:         to,
			Nonce:      nonce,
			Value:      val,
			GasPrice:   gasPrice,
			GasLimit:   gasLimit,
			ReadSlots:  readSlots,
			WriteSlots: writeSlots,
		}
	}

	return txs
}

// GenerateSyntheticEVMMempool creates a deterministic, realistic mixed DeFi EVM mempool batch
// featuring AMM swaps (with an adversarial sandwich sequence when count >= 6), ERC-20 transfers,
// EIP-2930 access lists, multi-nonce sequences, and native ETH value transfers.
func GenerateSyntheticEVMMempool(count int, seed int64) []*evm.Transaction {
	if count <= 0 {
		return nil
	}
	rng := rand.New(rand.NewSource(seed))
	txs := make([]*evm.Transaction, 0, count)

	gwei := big.NewInt(1_000_000_000)
	mkGwei := func(n int64) *big.Int {
		return new(big.Int).Mul(big.NewInt(n), gwei)
	}

	routerA := "0x7a250d5630b4cf539739df2c5dacb4c659f2488d"
	routerB := "0xe592427a0aece92de3edee1f18e0157c05861564"
	tokenUSDC := "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
	tokenDAI := "0x6b175474e89094c44da98b954eedeac495271d0f"
	vault := "0xba12222222228d8ba445958a75a0704d566bf2c8"

	nonces := make(map[string]uint64)
	nextNonce := func(addr string) uint64 {
		n := nonces[addr]
		nonces[addr] = n + 1
		return n
	}

	// Inject a realistic adversarial sandwich pattern on routerA when batch size allows.
	if count >= 6 {
		attacker := "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		victim := "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		swapAmt := big.NewInt(1_000_000)

		txs = append(txs, evm.NewValueTransaction(
			"", attacker, routerA, nextNonce(attacker),
			nil, mkGwei(120), mkGwei(60), 165_000,
			evm.EncodeUniswapSwap(swapAmt), nil,
		))
		txs = append(txs, evm.NewValueTransaction(
			"", victim, routerA, nextNonce(victim),
			nil, mkGwei(100), mkGwei(20), 180_000,
			evm.EncodeUniswapSwap(big.NewInt(5_000_000)), nil,
		))
		txs = append(txs, evm.NewValueTransaction(
			"", attacker, routerA, nextNonce(attacker),
			nil, mkGwei(100), mkGwei(20), 155_000,
			evm.EncodeUniswapSwap(swapAmt), nil,
		))
	}

	numAccounts := count / 3
	if numAccounts < 8 {
		numAccounts = 8
	}
	accounts := make([]string, numAccounts)
	for i := 0; i < numAccounts; i++ {
		accounts[i] = fmt.Sprintf("0x%040x", 0x10000+i)
	}

	tokens := []string{tokenUSDC, tokenDAI}
	routers := []string{routerA, routerB}

	for len(txs) < count {
		sender := accounts[rng.Intn(numAccounts)]
		recipient := accounts[rng.Intn(numAccounts)]
		if recipient == sender {
			recipient = accounts[(rng.Intn(numAccounts-1)+1)%numAccounts]
		}
		nonce := nextNonce(sender)
		baseFee := int64(25 + rng.Intn(40))
		tip := int64(2 + rng.Intn(25))
		roll := rng.Intn(100)

		switch {
		case roll < 42:
			// ERC-20 transfer (fine-grained user balance slots; parallelizable on same token contract)
			tok := tokens[rng.Intn(len(tokens))]
			amt := big.NewInt(int64(100 + rng.Intn(10_000)))
			txs = append(txs, evm.NewValueTransaction(
				"", sender, tok, nonce,
				nil, mkGwei(baseFee+tip), mkGwei(tip), uint64(55_000+rng.Intn(20_000)),
				evm.EncodeERC20Transfer(recipient, amt), nil,
			))

		case roll < 68:
			// Native ETH transfer (value-bearing; triggers balance RAW/WAW hazards on overlapping accounts)
			val := new(big.Int).Mul(big.NewInt(int64(1+rng.Intn(50))), big.NewInt(1_000_000_000_000_000)) // 0.001-0.05 ETH
			txs = append(txs, evm.NewValueTransaction(
				"", sender, recipient, nonce,
				val, mkGwei(baseFee+tip), mkGwei(tip), evm.DefaultTransferGas,
				nil, nil,
			))

		case roll < 82:
			// EIP-2930 vault interaction touching specific storage slots
			slot := fmt.Sprintf("0x%064x", rng.Intn(12))
			al := []evm.AccessTuple{
				{Address: vault, StorageKeys: []string{slot}},
			}
			txs = append(txs, evm.NewValueTransaction(
				"", sender, vault, nonce,
				nil, mkGwei(baseFee+tip), mkGwei(tip), uint64(90_000+rng.Intn(70_000)),
				[]byte{0xca, 0xfe, 0xba, 0xbe}, al,
			))

		case roll < 92:
			// ERC-20 approve (disjoint allowance slots)
			tok := tokens[rng.Intn(len(tokens))]
			spender := routers[rng.Intn(len(routers))]
			txs = append(txs, evm.NewValueTransaction(
				"", sender, tok, nonce,
				nil, mkGwei(baseFee+tip), mkGwei(tip), 46_000,
				evm.EncodeERC20Approve(spender, big.NewInt(1_000_000_000)), nil,
			))

		default:
			// AMM swap on routerA or routerB (high-gas serial pool contention)
			router := routers[rng.Intn(len(routers))]
			amt := big.NewInt(int64(10_000 + rng.Intn(500_000)))
			txs = append(txs, evm.NewValueTransaction(
				"", sender, router, nonce,
				nil, mkGwei(baseFee+tip), mkGwei(tip), uint64(140_000+rng.Intn(120_000)),
				evm.EncodeUniswapSwap(amt), nil,
			))
		}
	}

	return txs
}

// DumpMempoolJSON serializes scheduler transactions to an output writer.
func DumpMempoolJSON(w io.Writer, txs []*Transaction) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(txs)
}

// ExportMempoolFile writes a scheduler mempool batch to disk.
func ExportMempoolFile(path string, txs []*Transaction) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return DumpMempoolJSON(f, txs)
}

func parseBigIntFlex(raw json.RawMessage) (*big.Int, error) {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || trimmed == "null" || trimmed == `""` {
		return nil, nil
	}
	if len(trimmed) >= 2 && trimmed[0] == '"' && trimmed[len(trimmed)-1] == '"' {
		trimmed = strings.TrimSpace(trimmed[1 : len(trimmed)-1])
		if trimmed == "" {
			return nil, nil
		}
	}
	base := 10
	if strings.HasPrefix(trimmed, "0x") || strings.HasPrefix(trimmed, "0X") {
		trimmed = trimmed[2:]
		base = 16
	}
	val, ok := new(big.Int).SetString(trimmed, base)
	if !ok {
		return nil, fmt.Errorf("cannot parse integer %q", string(raw))
	}
	return val, nil
}

func parseHexBytesFlex(raw json.RawMessage) ([]byte, error) {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || trimmed == "null" || trimmed == `""` {
		return nil, nil
	}
	// Accept hex strings ("0x...") or standard JSON base64 strings/byte arrays.
	if trimmed[0] == '"' {
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil, err
		}
		s = strings.TrimSpace(s)
		if s == "" || s == "0x" || s == "0X" {
			return nil, nil
		}
		if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
			hexStr := s[2:]
			if len(hexStr)%2 == 1 {
				hexStr = "0" + hexStr
			}
			return hex.DecodeString(hexStr)
		}
		// Try plain hex first if even-length hex digits, otherwise fall back to Go []byte base64.
		if isHexString(s) && len(s)%2 == 0 {
			return hex.DecodeString(s)
		}
		var b []byte
		if err := json.Unmarshal(raw, &b); err == nil {
			return b, nil
		}
		return hex.DecodeString(s)
	}
	var b []byte
	if err := json.Unmarshal(raw, &b); err != nil {
		return nil, err
	}
	return b, nil
}

func isHexString(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return len(s) > 0
}
