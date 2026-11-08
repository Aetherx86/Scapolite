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
)

// IngestMempoolJSON parses an array of transactions from a reader or file.
func IngestMempoolJSON(r io.Reader) ([]*Transaction, error) {
	var txs []*Transaction
	dec := json.NewDecoder(r)
	if err := dec.Decode(&txs); err != nil {
		return nil, fmt.Errorf("failed to decode mempool JSON: %w", err)
	}
	return txs, nil
}

// GenerateSyntheticMempool creates a deterministic batch of transactions for benchmarking.
func GenerateSyntheticMempool(count int, seed int64) []*Transaction {
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

		// 30% chance of accessing shared contract storage
		if rng.Float64() < 0.3 {
			slotID := fmt.Sprintf("slot_%d", rng.Intn(5))
			if rng.Float64() < 0.5 {
				writeSlots[to] = []string{slotID}
			} else {
				readSlots[to] = []string{slotID}
			}
		}

		txs[i] = &Transaction{
			Hash:       txHash,
			From:       from,
			To:         to,
			Nonce:      nonce,
			Value:      big.NewInt(int64(rng.Intn(10))),
			GasPrice:   gasPrice,
			GasLimit:   100_000,
			ReadSlots:  readSlots,
			WriteSlots: writeSlots,
		}
	}

	return txs
}

// DumpMempoolJSON serializes transactions to an output writer.
func DumpMempoolJSON(w io.Writer, txs []*Transaction) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(txs)
}

// ExportMempoolFile writes mempool to disk.
func ExportMempoolFile(path string, txs []*Transaction) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return DumpMempoolJSON(f, txs)
}
