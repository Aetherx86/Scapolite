package dag

import (
	"encoding/hex"
	"math/big"
	"testing"

	"github.com/Aetherx86/Scapolite/pkg/evm"
)

func TestIndependentTransactions(t *testing.T) {
	// 4 transactions between completely independent senders and receivers
	tx1 := evm.NewTransaction("0x01", "0xaaa1", "0xbbb1", 0, big.NewInt(10), nil, nil, nil)
	tx2 := evm.NewTransaction("0x02", "0xaaa2", "0xbbb2", 0, big.NewInt(10), nil, nil, nil)
	tx3 := evm.NewTransaction("0x03", "0xaaa3", "0xbbb3", 0, big.NewInt(10), nil, nil, nil)
	tx4 := evm.NewTransaction("0x04", "0xaaa4", "0xbbb4", 0, big.NewInt(10), nil, nil, nil)

	dag := NewConflictDAG([]*evm.Transaction{tx1, tx2, tx3, tx4})
	if len(dag.Dependencies) != 0 {
		t.Fatalf("expected 0 dependencies, got %d", len(dag.Dependencies))
	}

	waves, err := dag.PartitionWaves()
	if err != nil {
		t.Fatalf("unexpected partition error: %v", err)
	}

	if len(waves) != 1 {
		t.Fatalf("expected 1 execution wave for independent txs, got %d", len(waves))
	}
	if len(waves[0].Transactions) != 4 {
		t.Fatalf("expected 4 txs in wave 0, got %d", len(waves[0].Transactions))
	}

	metrics := dag.ComputeMetrics(waves)
	if metrics.TheoreticalSpeedup != 4.0 {
		t.Fatalf("expected theoretical speedup 4.0, got %f", metrics.TheoreticalSpeedup)
	}
}

func TestSenderNonceSequence(t *testing.T) {
	// 3 transactions from the same sender
	tx1 := evm.NewTransaction("0x01", "0xaaa1", "0xbbb1", 0, big.NewInt(10), nil, nil, nil)
	tx2 := evm.NewTransaction("0x02", "0xaaa1", "0xbbb2", 1, big.NewInt(10), nil, nil, nil)
	tx3 := evm.NewTransaction("0x03", "0xaaa1", "0xbbb3", 2, big.NewInt(10), nil, nil, nil)

	dag := NewConflictDAG([]*evm.Transaction{tx1, tx2, tx3})
	if len(dag.Dependencies) != 3 { // tx1->tx2, tx1->tx3, tx2->tx3
		t.Fatalf("expected 3 nonce dependencies, got %d", len(dag.Dependencies))
	}

	waves, err := dag.PartitionWaves()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(waves) != 3 {
		t.Fatalf("expected 3 serialized waves, got %d", len(waves))
	}
}

func TestStorageSlotContention(t *testing.T) {
	// Tx1 and Tx2 access the same EIP-2930 storage slot in contract C
	contractC := "0xcccccccccccccccccccccccccccccccccccccccc"
	slotX := "0x000000000000000000000000000000000000000000000000000000000000000a"

	accessList := []evm.AccessTuple{
		{Address: contractC, StorageKeys: []string{slotX}},
	}

	tx1 := evm.NewTransaction("0x01", "0xaaa1", contractC, 0, big.NewInt(10), nil, nil, accessList)
	tx2 := evm.NewTransaction("0x02", "0xaaa2", contractC, 0, big.NewInt(10), nil, nil, accessList)
	tx3 := evm.NewTransaction("0x03", "0xaaa3", "0xbbb3", 0, big.NewInt(10), nil, nil, nil) // Independent

	dag := NewConflictDAG([]*evm.Transaction{tx1, tx2, tx3})
	waves, err := dag.PartitionWaves()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Tx1 and Tx3 can execute in Wave 0, Tx2 must execute in Wave 1
	if len(waves) != 2 {
		t.Fatalf("expected 2 waves, got %d", len(waves))
	}
	if len(waves[0].Transactions) != 2 {
		t.Fatalf("expected 2 txs in wave 0, got %d", len(waves[0].Transactions))
	}
	if len(waves[1].Transactions) != 1 {
		t.Fatalf("expected 1 tx in wave 1, got %d", len(waves[1].Transactions))
	}
}

func TestSandwichAttackDetection(t *testing.T) {
	// Attacker A, Victim V
	attacker := "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	victim := "0xvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv"
	router := "0x7a250d5630b4cf539739df2c5dacb4c659f2488d"

	// Uniswap swapExactTokensForTokens selector: 0x38ed1739
	swapData, _ := hex.DecodeString("38ed17390000000000000000000000000000000000000000000000000000000000000001")

	// TxA: Attacker Frontrun with high tip (50 gwei)
	txA := evm.NewTransaction("0x01", attacker, router, 0, big.NewInt(100), big.NewInt(50), swapData, nil)
	// TxB: Victim with normal tip (10 gwei)
	txB := evm.NewTransaction("0x02", victim, router, 0, big.NewInt(100), big.NewInt(10), swapData, nil)
	// TxC: Attacker Backrun with normal tip (10 gwei)
	txC := evm.NewTransaction("0x03", attacker, router, 1, big.NewInt(100), big.NewInt(10), swapData, nil)

	dag := NewConflictDAG([]*evm.Transaction{txA, txB, txC})
	detector := NewMEVDetector(dag)
	alerts := detector.DetectThreats()

	var sandwichFound bool
	for _, alert := range alerts {
		if alert.Type == MEVSandwich {
			sandwichFound = true
			if len(alert.TxHashes) != 3 {
				t.Fatalf("expected 3 transactions in sandwich, got %d", len(alert.TxHashes))
			}
		}
	}

	if !sandwichFound {
		t.Fatalf("expected sandwich attack to be detected, found alerts: %+v", alerts)
	}
}

func BenchmarkDAGConstruction(b *testing.B) {
	txs := make([]*evm.Transaction, 100)
	for i := 0; i < 100; i++ {
		txs[i] = evm.NewTransaction(
			"",
			"0xsender",
			"0xreceiver",
			uint64(i),
			big.NewInt(10),
			big.NewInt(2),
			nil,
			nil,
		)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = NewConflictDAG(txs)
	}
}

func BenchmarkWavePartitioning(b *testing.B) {
	txs := make([]*evm.Transaction, 50)
	for i := 0; i < 50; i++ {
		txs[i] = evm.NewTransaction(
			"",
			"0xsender",
			"0xreceiver",
			uint64(i),
			big.NewInt(10),
			big.NewInt(2),
			nil,
			nil,
		)
	}
	dag := NewConflictDAG(txs)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = dag.PartitionWaves()
	}
}
