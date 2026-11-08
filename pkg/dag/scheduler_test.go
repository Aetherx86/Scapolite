package dag

import (
	"testing"
)

func TestIndependentTransactions(t *testing.T) {
	sched := NewScheduler("deep")

	// 3 completely independent transactions
	tx1 := &Transaction{Hash: "0x1", From: "0xA", To: "0xB", Nonce: 0}
	tx2 := &Transaction{Hash: "0x2", From: "0xC", To: "0xD", Nonce: 0}
	tx3 := &Transaction{Hash: "0x3", From: "0xE", To: "0xF", Nonce: 0}

	sched.AddTransaction(tx1)
	sched.AddTransaction(tx2)
	sched.AddTransaction(tx3)

	report := sched.BuildGraph()

	if report.IndependentWaves != 1 {
		t.Errorf("expected 1 wave for independent txs, got %d", report.IndependentWaves)
	}
	if report.MaxConcurrency != 3 {
		t.Errorf("expected max concurrency 3, got %d", report.MaxConcurrency)
	}
	if report.TheoreticalSpeedup != 3.0 {
		t.Errorf("expected speedup 3.0, got %f", report.TheoreticalSpeedup)
	}
}

func TestNonceDependencies(t *testing.T) {
	sched := NewScheduler("deep")

	// Same sender with sequential nonces
	tx1 := &Transaction{Hash: "0x1", From: "0xA", To: "0xB", Nonce: 0}
	tx2 := &Transaction{Hash: "0x2", From: "0xA", To: "0xC", Nonce: 1}
	tx3 := &Transaction{Hash: "0x3", From: "0xA", To: "0xD", Nonce: 2}

	sched.AddTransaction(tx1)
	sched.AddTransaction(tx2)
	sched.AddTransaction(tx3)

	report := sched.BuildGraph()

	if report.IndependentWaves != 3 {
		t.Errorf("expected 3 sequential waves, got %d", report.IndependentWaves)
	}
	if report.NonceDependencies != 2 {
		t.Errorf("expected 2 nonce dependency edges, got %d", report.NonceDependencies)
	}
}

func TestStorageSlotContention(t *testing.T) {
	sched := NewScheduler("deep")

	// Tx1 writes to ContractX:slot_A
	tx1 := &Transaction{
		Hash:       "0x1",
		From:       "0xA",
		To:         "0xContractX",
		Nonce:      0,
		WriteSlots: map[string][]string{"0xContractX": {"slot_A"}},
	}

	// Tx2 reads from ContractX:slot_A
	tx2 := &Transaction{
		Hash:      "0x2",
		From:      "0xB",
		To:        "0xContractX",
		Nonce:     0,
		ReadSlots: map[string][]string{"0xContractX": {"slot_A"}},
	}

	// Tx3 accesses unrelated ContractY
	tx3 := &Transaction{
		Hash:       "0x3",
		From:       "0xC",
		To:         "0xContractY",
		Nonce:      0,
		WriteSlots: map[string][]string{"0xContractY": {"slot_B"}},
	}

	sched.AddTransaction(tx1)
	sched.AddTransaction(tx2)
	sched.AddTransaction(tx3)

	report := sched.BuildGraph()

	if report.IndependentWaves != 2 {
		t.Errorf("expected 2 waves, got %d", report.IndependentWaves)
	}
	if report.StateConflicts != 1 {
		t.Errorf("expected 1 state conflict edge, got %d", report.StateConflicts)
	}

	waves, err := sched.ComputeParallelWaves()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Wave 0 should have Tx1 and Tx3
	if len(waves[0]) != 2 {
		t.Errorf("expected 2 txs in Wave 0, got %d", len(waves[0]))
	}
	// Wave 1 should have Tx2
	if len(waves[1]) != 1 || waves[1][0].Hash != "0x2" {
		t.Errorf("expected Tx2 in Wave 1")
	}
}

func TestFastModeAddressContention(t *testing.T) {
	sched := NewScheduler("fast")

	tx1 := &Transaction{Hash: "0x1", From: "0xA", To: "0xUniswapPool", Nonce: 0}
	tx2 := &Transaction{Hash: "0x2", From: "0xB", To: "0xUniswapPool", Nonce: 0}
	tx3 := &Transaction{Hash: "0x3", From: "0xC", To: "0xCurvePool", Nonce: 0}

	sched.AddTransaction(tx1)
	sched.AddTransaction(tx2)
	sched.AddTransaction(tx3)

	report := sched.BuildGraph()

	if report.IndependentWaves != 2 {
		t.Errorf("expected 2 waves, got %d", report.IndependentWaves)
	}
	if report.StateConflicts != 1 {
		t.Errorf("expected 1 address contention conflict, got %d", report.StateConflicts)
	}
}

func TestSyntheticMempoolAndTopologicalSort(t *testing.T) {
	txs := GenerateSyntheticMempool(100, 42)
	if len(txs) != 100 {
		t.Fatalf("expected 100 txs, got %d", len(txs))
	}

	sched := NewScheduler("deep")
	for _, tx := range txs {
		sched.AddTransaction(tx)
	}

	report := sched.BuildGraph()
	if report.CyclesDetected {
		t.Errorf("cycles detected in generated synthetic mempool")
	}

	sorted, err := sched.TopologicalSort()
	if err != nil {
		t.Fatalf("TopologicalSort failed: %v", err)
	}
	if len(sorted) != 100 {
		t.Errorf("expected 100 sorted transactions, got %d", len(sorted))
	}

	if report.TheoreticalSpeedup <= 1.0 {
		t.Errorf("expected speedup > 1.0, got %f", report.TheoreticalSpeedup)
	}
}

func BenchmarkBuildGraph_1000Tx(b *testing.B) {
	txs := GenerateSyntheticMempool(1000, 1337)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		sched := NewScheduler("deep")
		for _, tx := range txs {
			sched.AddTransaction(tx)
		}
		_ = sched.BuildGraph()
	}
}

func BenchmarkComputeParallelWaves_1000Tx(b *testing.B) {
	txs := GenerateSyntheticMempool(1000, 1337)
	sched := NewScheduler("fast")
	for _, tx := range txs {
		sched.AddTransaction(tx)
	}
	_ = sched.BuildGraph()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = sched.ComputeParallelWaves()
	}
}
