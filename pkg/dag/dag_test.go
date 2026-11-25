package dag

import (
	"bytes"
	"encoding/hex"
	"math/big"
	"strings"
	"sync"
	"testing"

	"github.com/omakasekim/Scapolite/pkg/evm"
)

func TestConflictDAGIndependentTransactions(t *testing.T) {
	tx1 := evm.NewTransaction("0x01", "0xaaa1", "0xbbb1", 0, big.NewInt(10), nil, nil, nil)
	tx2 := evm.NewTransaction("0x02", "0xaaa2", "0xbbb2", 0, big.NewInt(10), nil, nil, nil)
	tx3 := evm.NewTransaction("0x03", "0xaaa3", "0xbbb3", 0, big.NewInt(10), nil, nil, nil)
	tx4 := evm.NewTransaction("0x04", "0xaaa4", "0xbbb4", 0, big.NewInt(10), nil, nil, nil)

	d := NewConflictDAG([]*evm.Transaction{tx1, tx2, tx3, tx4})
	if len(d.Dependencies) != 0 {
		t.Fatalf("expected 0 dependencies, got %d", len(d.Dependencies))
	}

	waves, err := d.PartitionWaves()
	if err != nil {
		t.Fatalf("unexpected partition error: %v", err)
	}
	if len(waves) != 1 {
		t.Fatalf("expected 1 execution wave for independent txs, got %d", len(waves))
	}
	if len(waves[0].Transactions) != 4 {
		t.Fatalf("expected 4 txs in wave 0, got %d", len(waves[0].Transactions))
	}

	metrics := d.ComputeMetrics(waves)
	if metrics.TheoreticalSpeedup != 4.0 {
		t.Fatalf("expected theoretical speedup 4.0, got %f", metrics.TheoreticalSpeedup)
	}
	if metrics.GasWeightedSpeedup != 4.0 {
		t.Fatalf("expected gas-weighted speedup 4.0, got %f", metrics.GasWeightedSpeedup)
	}
	if metrics.ParallelEfficiency != 1.0 {
		t.Fatalf("expected parallel efficiency 1.0, got %f", metrics.ParallelEfficiency)
	}
}

func TestConflictDAGSenderNonceOrderingAndAnomalies(t *testing.T) {
	// Out-of-order nonce submission with a gap (0 -> 2) and a duplicate nonce (2 with higher/lower tip)
	// plus overlapping shared slot access: must NEVER produce a spurious cycle.
	contract := "0xcccccccccccccccccccccccccccccccccccccccc"
	al := []evm.AccessTuple{{Address: contract, StorageKeys: []string{"0x01"}}}

	txNonce2Low := evm.NewTransaction("0x2lo", "0xaaa1", contract, 2, big.NewInt(20), big.NewInt(5), nil, al)
	txNonce0 := evm.NewTransaction("0x00", "0xaaa1", contract, 0, big.NewInt(20), big.NewInt(5), nil, al)
	txNonce2High := evm.NewTransaction("0x2hi", "0xaaa1", contract, 2, big.NewInt(50), big.NewInt(40), nil, al)

	d := NewConflictDAG([]*evm.Transaction{txNonce2Low, txNonce0, txNonce2High})
	waves, err := d.PartitionWaves()
	if err != nil {
		t.Fatalf("out-of-order nonces must not create cycles: %v", err)
	}
	if len(waves) != 3 {
		t.Fatalf("expected 3 serialized waves, got %d", len(waves))
	}
	// Canonical order: nonce 0 -> nonce 2 (high tip RBF winner) -> nonce 2 (low tip superseded)
	if waves[0].Transactions[0] != "0x00" || waves[1].Transactions[0] != "0x2hi" || waves[2].Transactions[0] != "0x2lo" {
		t.Fatalf("unexpected wave order: %+v", waves)
	}

	metrics := d.ComputeMetrics(waves)
	if metrics.NonceGaps != 1 {
		t.Errorf("expected 1 nonce gap, got %d", metrics.NonceGaps)
	}
	if metrics.NonceConflicts != 1 {
		t.Errorf("expected 1 duplicate nonce conflict, got %d", metrics.NonceConflicts)
	}

	anomalies := d.NonceAnomalies()
	if len(anomalies) != 2 {
		t.Fatalf("expected 2 nonce anomalies (gap + duplicate), got %+v", anomalies)
	}
}

func TestReadAfterWriteConcurrentReadersThenWrite(t *testing.T) {
	// W1 -> {R1, R2 in parallel} -> W2 on the same storage slot.
	sched := NewScheduler("deep")
	sched.AddTransaction(&Transaction{
		Hash: "0xw1", From: "0xa1", To: "0xc1", Nonce: 0,
		WriteSlots: map[string][]string{"0xc1": {"slot_0"}},
	})
	sched.AddTransaction(&Transaction{
		Hash: "0xr1", From: "0xa2", To: "0xc1", Nonce: 0,
		ReadSlots: map[string][]string{"0xc1": {"slot_0"}},
	})
	sched.AddTransaction(&Transaction{
		Hash: "0xr2", From: "0xa3", To: "0xc1", Nonce: 0,
		ReadSlots: map[string][]string{"0xc1": {"slot_0"}},
	})
	sched.AddTransaction(&Transaction{
		Hash: "0xw2", From: "0xa4", To: "0xc1", Nonce: 0,
		WriteSlots: map[string][]string{"0xc1": {"slot_0"}},
	})

	report := sched.BuildGraph()
	if report.IndependentWaves != 3 {
		t.Fatalf("expected 3 waves (W1 -> [R1, R2] -> W2), got %d", report.IndependentWaves)
	}
	// Linear last-writer / concurrent-readers edges: W1->R1, W1->R2 (RAW), R1->W2, R2->W2 (WAR) = 4 edges.
	if report.StateConflicts != 4 {
		t.Fatalf("expected 4 minimal RAW/WAR conflict edges, got %d", report.StateConflicts)
	}
	if report.HazardBreakdown[DepReadWrite] != 2 || report.HazardBreakdown[DepWriteRead] != 2 {
		t.Fatalf("unexpected hazard breakdown: %+v", report.HazardBreakdown)
	}

	waves, err := sched.ComputeParallelWaves()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(waves[0]) != 1 || waves[0][0].Hash != "0xw1" {
		t.Fatalf("expected W1 alone in wave 0, got %+v", waves[0])
	}
	if len(waves[1]) != 2 {
		t.Fatalf("expected R1 and R2 executing concurrently in wave 1, got %d txs", len(waves[1]))
	}
	if len(waves[2]) != 1 || waves[2][0].Hash != "0xw2" {
		t.Fatalf("expected W2 alone in wave 2, got %+v", waves[2])
	}
}

func TestBalanceHazardsAndOverdraftViolations(t *testing.T) {
	alice := "0x1111111111111111111111111111111111111111"
	bob := "0x2222222222222222222222222222222222222222"
	carol := "0x3333333333333333333333333333333333333333"

	// Tx1: Alice sends 1000 wei to Bob.
	// Tx2: Bob spends 800 wei to Carol (depends on Tx1 crediting Bob!).
	// Tx3: Carol attempts to spend 5000 wei (exceeds Carol's 800 wei -> BALANCE_VIOLATION).
	tx1 := evm.NewValueTransaction("0x1", alice, bob, 0, big.NewInt(1000), big.NewInt(0), nil, 21000, nil, nil)
	tx2 := evm.NewValueTransaction("0x2", bob, carol, 0, big.NewInt(800), big.NewInt(0), nil, 21000, nil, nil)
	tx3 := evm.NewValueTransaction("0x3", carol, alice, 0, big.NewInt(5000), big.NewInt(0), nil, 21000, nil, nil)

	balances := map[string]*big.Int{
		alice: big.NewInt(2000),
		bob:   big.NewInt(0),
		carol: big.NewInt(0),
	}

	d := NewConflictDAGWithBalances([]*evm.Transaction{tx1, tx2, tx3}, "deep", balances)
	waves, err := d.PartitionWaves()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(waves) != 3 {
		t.Fatalf("expected 3 serialized balance-dependent waves, got %d", len(waves))
	}

	metrics := d.ComputeMetrics(waves)
	if metrics.BalanceHazards < 2 {
		t.Errorf("expected at least 2 balance hazard edges, got %d", metrics.BalanceHazards)
	}
	if metrics.BalanceViolations != 1 {
		t.Errorf("expected 1 balance violation (carol overdraft), got %d", metrics.BalanceViolations)
	}

	violations := d.BalanceViolations()
	if len(violations) != 1 || violations[0].Account != carol || violations[0].TxHash != "0x3" {
		t.Fatalf("unexpected balance violations: %+v", violations)
	}
}

func TestDeepVsFastERC20Parallelism(t *testing.T) {
	token := "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
	txs := []*evm.Transaction{
		evm.NewTransaction("0x1", "0x0000000000000000000000000000000000000001", token, 0, big.NewInt(10), nil,
			evm.EncodeERC20Transfer("0x0000000000000000000000000000000000000101", big.NewInt(100)), nil),
		evm.NewTransaction("0x2", "0x0000000000000000000000000000000000000002", token, 0, big.NewInt(10), nil,
			evm.EncodeERC20Transfer("0x0000000000000000000000000000000000000102", big.NewInt(100)), nil),
		evm.NewTransaction("0x3", "0x0000000000000000000000000000000000000003", token, 0, big.NewInt(10), nil,
			evm.EncodeERC20Transfer("0x0000000000000000000000000000000000000103", big.NewInt(100)), nil),
	}

	fastDAG := NewConflictDAGWithMode(txs, "fast")
	fastWaves, err := fastDAG.PartitionWaves()
	if err != nil {
		t.Fatalf("fast mode error: %v", err)
	}
	if len(fastWaves) != 3 {
		t.Fatalf("fast mode expected 3 sequential waves on shared token contract, got %d", len(fastWaves))
	}

	deepDAG := NewConflictDAGWithMode(txs, "deep")
	deepWaves, err := deepDAG.PartitionWaves()
	if err != nil {
		t.Fatalf("deep mode error: %v", err)
	}
	if len(deepWaves) != 1 {
		t.Fatalf("deep mode expected 1 parallel wave for disjoint ERC20 transfers, got %d", len(deepWaves))
	}
}

func TestGasWeightedCriticalPathAndWorkStealingEfficiency(t *testing.T) {
	// Chain A: 3 short ETH transfers (21k each = 63k gas, 3 hops) via same sender.
	// Chain B: 2 heavy swaps (500k each = 1,000k gas, 2 hops) via shared slot.
	// Independent straggler + follower:
	//   txFast (20k) -> txFollow (180k) alongside txHeavy (200k).
	sched := NewScheduler("deep")

	// Chain A (3 hops, 63k gas)
	sched.AddTransaction(&Transaction{Hash: "0xa1", From: "0xA", To: "0x1", Nonce: 0, GasLimit: 21_000})
	sched.AddTransaction(&Transaction{Hash: "0xa2", From: "0xA", To: "0x2", Nonce: 1, GasLimit: 21_000})
	sched.AddTransaction(&Transaction{Hash: "0xa3", From: "0xA", To: "0x3", Nonce: 2, GasLimit: 21_000})

	// Chain B (2 hops, 1,000k gas)
	sched.AddTransaction(&Transaction{
		Hash: "0xb1", From: "0xB1", To: "0xPool", Nonce: 0, GasLimit: 500_000,
		WriteSlots: map[string][]string{"0xPool": {"reserve"}},
	})
	sched.AddTransaction(&Transaction{
		Hash: "0xb2", From: "0xB2", To: "0xPool", Nonce: 0, GasLimit: 500_000,
		WriteSlots: map[string][]string{"0xPool": {"reserve"}},
	})

	analysis, err := sched.Analyze(2, 5)
	if err != nil {
		t.Fatalf("Analyze failed: %v", err)
	}

	if analysis.CriticalPath.HopLength != 3 {
		t.Errorf("expected hop critical path length 3, got %d", analysis.CriticalPath.HopLength)
	}
	if len(analysis.CriticalPath.HopChain) != 3 {
		t.Errorf("expected hop chain len 3, got %v", analysis.CriticalPath.HopChain)
	}
	if analysis.CriticalPath.GasWeight != 1_000_000 {
		t.Errorf("expected gas critical path weight 1000000, got %d", analysis.CriticalPath.GasWeight)
	}
	if len(analysis.CriticalPath.GasChain) != 2 || analysis.CriticalPath.GasChain[0] != "0xb1" || analysis.CriticalPath.GasChain[1] != "0xb2" {
		t.Errorf("unexpected gas critical chain: %v", analysis.CriticalPath.GasChain)
	}
	if len(analysis.CriticalPath.BottleneckKeys) == 0 {
		t.Errorf("expected bottleneck key for 0xPool:reserve")
	}

	// Wave-barrier vs work-stealing on asymmetric straggler workload:
	// tx1 (200k) independent; tx2 (20k) -> tx3 (180k) sequential.
	asym := NewScheduler("deep")
	asym.AddTransaction(&Transaction{Hash: "0x1", From: "0xA", To: "0xD1", Nonce: 0, GasLimit: 200_000})
	asym.AddTransaction(&Transaction{Hash: "0x2", From: "0xB", To: "0xD2", Nonce: 0, GasLimit: 20_000})
	asym.AddTransaction(&Transaction{Hash: "0x3", From: "0xB", To: "0xD3", Nonce: 1, GasLimit: 180_000})
	asym.BuildGraph()

	sim, err := asym.SimulateWorkers(2)
	if err != nil {
		t.Fatalf("SimulateWorkers failed: %v", err)
	}
	if sim.SequentialGas != 400_000 {
		t.Fatalf("expected sequential gas 400000, got %d", sim.SequentialGas)
	}
	// Wave 0 = max(200k, 20k) = 200k; Wave 1 = 180k -> waveBarrier = 380k
	if sim.WaveBarrierGas != 380_000 {
		t.Fatalf("expected wave barrier gas 380000, got %d", sim.WaveBarrierGas)
	}
	// Work-stealing: worker0 runs tx1 [0..200k], worker1 runs tx2 [0..20k] then tx3 [20k..200k] -> 200k!
	if sim.WorkStealingGas != 200_000 {
		t.Fatalf("expected work stealing gas 200000, got %d", sim.WorkStealingGas)
	}
	if sim.WorkStealingSpeedup <= sim.WaveBarrierSpeedup {
		t.Fatalf("expected work-stealing speedup (%.2fx) > wave-barrier speedup (%.2fx)",
			sim.WorkStealingSpeedup, sim.WaveBarrierSpeedup)
	}
}

func TestExplicitCycleDetection(t *testing.T) {
	sched := NewScheduler("deep")
	sched.AddTransaction(&Transaction{Hash: "0x1", From: "0xA", To: "0xB", Nonce: 0})
	sched.AddTransaction(&Transaction{Hash: "0x2", From: "0xC", To: "0xD", Nonce: 0})
	sched.BuildGraph()

	if err := sched.AddExplicitDependency("0x1", "0x2", DepReadWrite, "manual edge 1->2"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := sched.AddExplicitDependency("0x2", "0x1", DepReadWrite, "manual edge 2->1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := sched.AddExplicitDependency("0xmissing", "0x1", DepReadWrite, "bad"); err != ErrTransactionNotFound {
		t.Fatalf("expected ErrTransactionNotFound, got %v", err)
	}

	if _, err := sched.ComputeParallelWaves(); err != ErrCyclicDependency {
		t.Fatalf("expected ErrCyclicDependency, got %v", err)
	}
	if _, err := sched.TopologicalSort(); err != ErrCyclicDependency {
		t.Fatalf("expected ErrCyclicDependency from TopologicalSort, got %v", err)
	}
}

func TestMEVDetectorSandwichFrontrunBackrunAndHotspot(t *testing.T) {
	attacker := "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	victim := "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	arbBot := "0xcccccccccccccccccccccccccccccccccccccccc"
	trader := "0xdddddddddddddddddddddddddddddddddddddddd"
	router := "0x7a250d5630b4cf539739df2c5dacb4c659f2488d"

	swapData, _ := hex.DecodeString("38ed17390000000000000000000000000000000000000000000000000000000000000001")

	// 1. Sandwich: attacker frontrun (tip 60) -> victim (tip 20) -> attacker backrun (tip 20)
	txFront := evm.NewTransaction("0x01", attacker, router, 0, big.NewInt(100), big.NewInt(60), swapData, nil)
	txVictim := evm.NewTransaction("0x02", victim, router, 0, big.NewInt(100), big.NewInt(20), swapData, nil)
	txBack := evm.NewTransaction("0x03", attacker, router, 1, big.NewInt(100), big.NewInt(20), swapData, nil)
	// 2. Arbitrage backrun: trader swap (tip 30) immediately followed by arbBot matching tip (29, within 5%)
	txTrader := evm.NewTransaction("0x04", trader, router, 0, big.NewInt(100), big.NewInt(30), swapData, nil)
	txArb := evm.NewTransaction("0x05", arbBot, router, 0, big.NewInt(100), big.NewInt(29), swapData, nil)

	d := NewConflictDAG([]*evm.Transaction{txFront, txVictim, txBack, txTrader, txArb})
	detector := NewMEVDetector(d)
	alerts := detector.DetectThreats()

	found := make(map[MEVThreatType]bool)
	for _, a := range alerts {
		found[a.Type] = true
	}

	if !found[MEVSandwich] {
		t.Errorf("expected %s alert, got %+v", MEVSandwich, alerts)
	}
	if !found[MEVFrontrun] {
		t.Errorf("expected %s alert, got %+v", MEVFrontrun, alerts)
	}
	if !found[MEVBackrun] {
		t.Errorf("expected %s alert, got %+v", MEVBackrun, alerts)
	}
	if !found[MEVContention] {
		t.Errorf("expected %s alert (5 txs on router), got %+v", MEVContention, alerts)
	}

	// Verify deterministic severity ordering (HIGH before MEDIUM before LOW).
	for i := 1; i < len(alerts); i++ {
		if severityRank(alerts[i-1].Severity) > severityRank(alerts[i].Severity) {
			t.Errorf("alerts not sorted by severity: %s before %s", alerts[i-1].Severity, alerts[i].Severity)
		}
	}
}

func TestEVMMempoolJSONDualFormatRoundTrip(t *testing.T) {
	orig := GenerateSyntheticEVMMempool(30, 99)
	var buf bytes.Buffer
	if err := DumpEVMMempoolJSON(&buf, orig); err != nil {
		t.Fatalf("DumpEVMMempoolJSON failed: %v", err)
	}

	parsed, err := IngestEVMMempoolJSON(&buf)
	if err != nil {
		t.Fatalf("IngestEVMMempoolJSON failed: %v", err)
	}
	if len(parsed) != len(orig) {
		t.Fatalf("round-trip count mismatch: got %d, want %d", len(parsed), len(orig))
	}
	for i := range orig {
		if parsed[i].Hash != orig[i].Hash || parsed[i].Action != orig[i].Action {
			t.Fatalf("tx[%d] mismatch after round-trip: got (%s, %s), want (%s, %s)",
				i, parsed[i].Hash, parsed[i].Action, orig[i].Hash, orig[i].Action)
		}
	}

	// Also verify ingestion of legacy Scapolite snake_case slot-footprint JSON + hex values.
	mixedJSON := `[
		{
			"hash": "0xabc1",
			"from": "0x1111",
			"to": "0x2222",
			"nonce": 0,
			"value": "0x64",
			"gas_price": 20000000000,
			"gas_limit": 85000,
			"read_slots": {"0x2222": ["slot_a"]},
			"write_slots": {"0x2222": ["slot_b"]},
			"balance_deltas": {"0x3333": "-50"}
		}
	]`
	mixed, err := IngestEVMMempoolJSON(strings.NewReader(mixedJSON))
	if err != nil {
		t.Fatalf("failed to parse snake_case/hex JSON: %v", err)
	}
	if len(mixed) != 1 || mixed[0].Value.Cmp(big.NewInt(100)) != 0 || mixed[0].GasLimit != 85000 {
		t.Fatalf("unexpected mixed parse result: %+v", mixed[0])
	}
	if _, ok := mixed[0].WriteSet["0x2222:slot_b"]; !ok {
		t.Fatalf("missing explicit write slot in derived WriteSet: %+v", mixed[0].WriteSet)
	}
}

func TestSchedulerConcurrentRaceSafety(t *testing.T) {
	txs := GenerateSyntheticEVMMempool(120, 777)
	sched := NewScheduler("deep")
	for _, tx := range txs {
		sched.AddEVMTransaction(tx)
	}
	_ = sched.BuildGraph()

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			if _, err := sched.ComputeParallelWaves(); err != nil {
				t.Errorf("goroutine %d ComputeParallelWaves error: %v", id, err)
			}
			if _, err := sched.ComputeCriticalPath(); err != nil {
				t.Errorf("goroutine %d ComputeCriticalPath error: %v", id, err)
			}
			if _, err := sched.SimulateWorkers(4); err != nil {
				t.Errorf("goroutine %d SimulateWorkers error: %v", id, err)
			}
			_ = sched.ComputeHotspots(5)
			_ = sched.Edges()
			_ = sched.NonceAnomalies()
			_ = sched.BalanceViolations()
		}(g)
	}
	wg.Wait()
}

func BenchmarkDAGConstruction(b *testing.B) {
	txs := GenerateSyntheticEVMMempool(500, 1337)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = NewConflictDAG(txs)
	}
}

func BenchmarkWavePartitioning(b *testing.B) {
	txs := GenerateSyntheticEVMMempool(500, 1337)
	d := NewConflictDAG(txs)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = d.PartitionWaves()
	}
}

func BenchmarkWorkerSimulation_500Tx(b *testing.B) {
	txs := GenerateSyntheticEVMMempool(500, 1337)
	d := NewConflictDAG(txs)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = d.SimulateWorkers(8)
	}
}
