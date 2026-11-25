package dag

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"sync"

	"github.com/omakasekim/Scapolite/pkg/evm"
)

var (
	ErrCyclicDependency    = errors.New("cyclic dependency detected in transaction graph")
	ErrTransactionNotFound = errors.New("transaction not found in graph")
)

type slotState struct {
	lastWriter string
	readers    []string
}

type hotspotAcc struct {
	writes int
	reads  int
	edges  int
	txSet  map[string]struct{}
	txs    []string
}

// Scheduler builds and schedules concurrent EVM transaction execution graphs.
// All exported methods are safe for concurrent use.
type Scheduler struct {
	mu                sync.RWMutex
	transactions      []*Transaction
	ordered           []*Transaction
	txMap             map[string]*Transaction
	orderIndex        map[string]int
	adj               map[string][]*Edge
	revAdj            map[string][]*Edge
	edges             []*Edge
	inDegree          map[string]int
	mode              string // "fast" or "deep"
	initialBalances   map[string]*big.Int
	nonceAnomalies    []NonceAnomaly
	balanceViolations []BalanceViolation
	hotspots          map[string]*hotspotAcc
	lastReport        *ConflictReport
}

// NewScheduler creates a transaction dependency scheduler in "fast" or "deep" mode.
func NewScheduler(mode string) *Scheduler {
	mode = normalizeMode(mode)
	return &Scheduler{
		txMap:           make(map[string]*Transaction),
		orderIndex:      make(map[string]int),
		adj:             make(map[string][]*Edge),
		revAdj:          make(map[string][]*Edge),
		inDegree:        make(map[string]int),
		mode:            mode,
		initialBalances: make(map[string]*big.Int),
		hotspots:        make(map[string]*hotspotAcc),
	}
}

func normalizeMode(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "deep":
		return "deep"
	default:
		return "fast"
	}
}

// Mode returns the scheduler's active conflict resolution mode ("fast" or "deep").
func (s *Scheduler) Mode() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mode
}

// SetAccountBalance registers an account's initial balance (in wei) for overdraft hazard checks.
func (s *Scheduler) SetAccountBalance(addr string, balance *big.Int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	norm := strings.ToLower(strings.TrimSpace(addr))
	if norm == "" || balance == nil {
		return
	}
	s.initialBalances[norm] = new(big.Int).Set(balance)
}

// SetAccountBalances registers multiple initial account balances (in wei).
func (s *Scheduler) SetAccountBalances(balances map[string]*big.Int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for addr, bal := range balances {
		norm := strings.ToLower(strings.TrimSpace(addr))
		if norm == "" || bal == nil {
			continue
		}
		s.initialBalances[norm] = new(big.Int).Set(bal)
	}
}

// AddTransaction appends a transaction to the scheduling pool.
func (s *Scheduler) AddTransaction(tx *Transaction) {
	if tx == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if tx.Hash == "" {
		h := sha256.Sum256([]byte(fmt.Sprintf("%s:%s:%d:%d", tx.From, tx.To, tx.Nonce, len(s.transactions))))
		tx.Hash = "0x" + hex.EncodeToString(h[:])
	}
	if _, exists := s.txMap[tx.Hash]; exists {
		return
	}

	s.transactions = append(s.transactions, tx)
	s.txMap[tx.Hash] = tx
	s.inDegree[tx.Hash] = 0
}

// AddEVMTransaction adapts and appends an evm.Transaction to the scheduling pool.
func (s *Scheduler) AddEVMTransaction(etx *evm.Transaction) {
	if etx == nil {
		return
	}
	s.AddTransaction(FromEVMTransaction(etx))
}

// AddExplicitDependency injects a manual dependency edge between two known transactions.
// Useful for custom builder ordering constraints or cycle detection verification.
func (s *Scheduler) AddExplicitDependency(fromHash, toHash string, depType DependencyType, detail string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.txMap[fromHash]; !ok {
		return ErrTransactionNotFound
	}
	if _, ok := s.txMap[toHash]; !ok {
		return ErrTransactionNotFound
	}
	if !s.hasEdgeLocked(fromHash, toHash) {
		s.addEdgeLocked(fromHash, toHash, depType, "", detail)
	}
	return nil
}

// BuildGraph constructs dependency edges between all ingested transactions and computes
// wave schedule, critical-path, and parallel efficiency metrics.
func (s *Scheduler) BuildGraph() *ConflictReport {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Reset graph state.
	s.adj = make(map[string][]*Edge, len(s.transactions))
	s.revAdj = make(map[string][]*Edge, len(s.transactions))
	s.edges = make([]*Edge, 0)
	s.inDegree = make(map[string]int, len(s.transactions))
	s.orderIndex = make(map[string]int, len(s.transactions))
	s.nonceAnomalies = nil
	s.balanceViolations = nil
	s.hotspots = make(map[string]*hotspotAcc)

	// Determine canonical execution order: each sender's transactions are ordered
	// by ascending nonce (tie-breaking duplicate nonces by descending effective tip),
	// slotted back into that sender's original mempool positions so cross-sender
	// arrival order is preserved while preventing spurious nonce-vs-slot cycles.
	s.ordered = s.canonicalOrderLocked()
	for idx, tx := range s.ordered {
		s.inDegree[tx.Hash] = 0
		s.orderIndex[tx.Hash] = idx
	}

	report := &ConflictReport{
		TotalTransactions: len(s.ordered),
		HazardBreakdown:   make(map[DependencyType]int),
	}
	for _, tx := range s.ordered {
		report.TotalGas += tx.EffectiveGasLimit()
	}

	// 1. Sender nonce sequence ordering, duplicate-nonce (RBF) conflict, and nonce-gap detection.
	s.buildNonceDependenciesLocked(report)

	// 2. State access conflict detection (fast address-level or deep storage-slot RAW/WAR/WAW).
	if s.mode == "fast" {
		s.buildFastConflictsLocked(report)
	} else {
		s.buildDeepSlotConflictsLocked(report)
	}

	// 3. Native ETH & explicit BalanceDelta hazard and overdraft violation detection.
	s.buildBalanceHazardsLocked(report)

	// 4. Schedule waves and compute critical-path and efficiency metrics.
	waves, err := s.computeParallelWavesLocked()
	if err != nil {
		report.CyclesDetected = true
	} else {
		s.populateMetricsLocked(report, waves)
	}

	s.lastReport = report
	return report
}

func (s *Scheduler) canonicalOrderLocked() []*Transaction {
	n := len(s.transactions)
	if n == 0 {
		return nil
	}

	ordered := make([]*Transaction, n)
	senderPositions := make(map[string][]int)
	senderTxs := make(map[string][]*Transaction)

	for i, tx := range s.transactions {
		sender := strings.ToLower(strings.TrimSpace(tx.From))
		senderPositions[sender] = append(senderPositions[sender], i)
		senderTxs[sender] = append(senderTxs[sender], tx)
	}

	for sender, txs := range senderTxs {
		positions := senderPositions[sender]
		if len(txs) == 1 {
			ordered[positions[0]] = txs[0]
			continue
		}

		// Stable sort by ascending nonce; for duplicate nonces, higher effective gas tip wins.
		type indexedTx struct {
			tx  *Transaction
			pos int
		}
		items := make([]indexedTx, len(txs))
		for i, tx := range txs {
			items[i] = indexedTx{tx: tx, pos: positions[i]}
		}
		sort.Slice(items, func(i, j int) bool {
			if items[i].tx.Nonce != items[j].tx.Nonce {
				return items[i].tx.Nonce < items[j].tx.Nonce
			}
			cmp := items[i].tx.EffectiveGasTip().Cmp(items[j].tx.EffectiveGasTip())
			if cmp != 0 {
				return cmp > 0
			}
			return items[i].pos < items[j].pos
		})
		for i, pos := range positions {
			ordered[pos] = items[i].tx
		}
	}

	return ordered
}

func (s *Scheduler) buildNonceDependenciesLocked(report *ConflictReport) {
	bySender := make(map[string][]*Transaction)
	senderOrder := make([]string, 0)
	for _, tx := range s.ordered {
		sender := strings.ToLower(strings.TrimSpace(tx.From))
		if sender == "" {
			continue
		}
		if _, exists := bySender[sender]; !exists {
			senderOrder = append(senderOrder, sender)
		}
		bySender[sender] = append(bySender[sender], tx)
	}

	for _, sender := range senderOrder {
		txs := bySender[sender]
		if len(txs) <= 1 {
			continue
		}

		// Group duplicate nonces for anomaly reporting.
		dupMap := make(map[uint64][]string)
		dupNonces := make([]uint64, 0)

		for i := 0; i < len(txs)-1; i++ {
			cur := txs[i]
			next := txs[i+1]
			key := evm.NonceKey(sender)

			if cur.Nonce == next.Nonce {
				if _, seen := dupMap[cur.Nonce]; !seen {
					dupMap[cur.Nonce] = append(dupMap[cur.Nonce], cur.Hash)
					dupNonces = append(dupNonces, cur.Nonce)
				}
				dupMap[cur.Nonce] = append(dupMap[cur.Nonce], next.Hash)

				if !s.hasEdgeLocked(cur.Hash, next.Hash) {
					s.addEdgeLocked(cur.Hash, next.Hash, DepNonceConflict, key,
						fmt.Sprintf("Duplicate nonce conflict: %s nonce %d (RBF priority)", sender, cur.Nonce))
					report.NonceDependencies++
					report.NonceConflicts++
					report.HazardBreakdown[DepNonceConflict]++
					s.recordHotspotEdgeLocked(key, cur.Hash, next.Hash, true)
				}
				continue
			}

			if next.Nonce > cur.Nonce+1 {
				s.nonceAnomalies = append(s.nonceAnomalies, NonceAnomaly{
					Sender:        sender,
					Kind:          "NONCE_GAP",
					Nonce:         next.Nonce,
					ExpectedNonce: cur.Nonce + 1,
					TxHashes:      []string{cur.Hash, next.Hash},
				})
				report.NonceGaps++
			}

			if !s.hasEdgeLocked(cur.Hash, next.Hash) {
				s.addEdgeLocked(cur.Hash, next.Hash, DepNonce, key,
					fmt.Sprintf("Nonce sequence: %s nonce %d -> %d", sender, cur.Nonce, next.Nonce))
				report.NonceDependencies++
				report.HazardBreakdown[DepNonce]++
			}
		}

		for _, n := range dupNonces {
			s.nonceAnomalies = append(s.nonceAnomalies, NonceAnomaly{
				Sender:   sender,
				Kind:     "DUPLICATE_NONCE",
				Nonce:    n,
				TxHashes: dupMap[n],
			})
		}
	}
}

func (s *Scheduler) buildFastConflictsLocked(report *ConflictReport) {
	lastAccessor := make(map[string]string) // normalized target address -> last txHash
	for _, tx := range s.ordered {
		target := strings.ToLower(strings.TrimSpace(tx.To))
		if target == "" {
			continue
		}
		key := evm.ContractKey(target)
		s.recordHotspotTouchLocked(key, tx.Hash, true)

		if prev, exists := lastAccessor[target]; exists && prev != tx.Hash {
			if !s.hasEdgeLocked(prev, tx.Hash) {
				s.addEdgeLocked(prev, tx.Hash, DepWriteWrite, key,
					fmt.Sprintf("Coarse contract address conflict on %s", target))
				report.StateConflicts++
				report.HazardBreakdown[DepWriteWrite]++
				s.recordHotspotEdgeLocked(key, prev, tx.Hash, true)
			}
		}
		lastAccessor[target] = tx.Hash
	}
}

func (s *Scheduler) buildDeepSlotConflictsLocked(report *ConflictReport) {
	states := make(map[string]*slotState)

	for _, tx := range s.ordered {
		readSet, writeSet := extractSlotSets(tx)

		// Process write (and read-write) slots in sorted order for deterministic edges.
		writeKeys := sortedSetKeys(writeSet)
		for _, key := range writeKeys {
			_, alsoReads := readSet[key]
			s.recordHotspotTouchLocked(key, tx.Hash, true)
			if alsoReads {
				s.recordHotspotTouchLocked(key, tx.Hash, false)
			}

			st := states[key]
			if st == nil {
				st = &slotState{lastWriter: tx.Hash}
				states[key] = st
				continue
			}

			if len(st.readers) > 0 {
				for _, reader := range st.readers {
					if reader == tx.Hash {
						continue
					}
					if !s.hasEdgeLocked(reader, tx.Hash) {
						s.addEdgeLocked(reader, tx.Hash, DepWriteRead, key,
							fmt.Sprintf("Slot contention on %s (%s)", key, DepWriteRead))
						report.StateConflicts++
						report.HazardBreakdown[DepWriteRead]++
						s.recordHotspotEdgeLocked(key, reader, tx.Hash, true)
					}
				}
			} else if st.lastWriter != "" && st.lastWriter != tx.Hash {
				depType := DepWriteWrite
				if alsoReads {
					depType = DepReadWrite
				}
				if !s.hasEdgeLocked(st.lastWriter, tx.Hash) {
					s.addEdgeLocked(st.lastWriter, tx.Hash, depType, key,
						fmt.Sprintf("Slot contention on %s (%s)", key, depType))
					report.StateConflicts++
					report.HazardBreakdown[depType]++
					s.recordHotspotEdgeLocked(key, st.lastWriter, tx.Hash, true)
				}
			}

			st.lastWriter = tx.Hash
			st.readers = nil
		}

		// Process read-only slots in sorted order.
		readOnlyKeys := make([]string, 0, len(readSet))
		for key := range readSet {
			if _, isWrite := writeSet[key]; !isWrite {
				readOnlyKeys = append(readOnlyKeys, key)
			}
		}
		sort.Strings(readOnlyKeys)

		for _, key := range readOnlyKeys {
			s.recordHotspotTouchLocked(key, tx.Hash, false)
			st := states[key]
			if st == nil {
				st = &slotState{readers: []string{tx.Hash}}
				states[key] = st
				continue
			}

			if st.lastWriter != "" && st.lastWriter != tx.Hash {
				if !s.hasEdgeLocked(st.lastWriter, tx.Hash) {
					s.addEdgeLocked(st.lastWriter, tx.Hash, DepReadWrite, key,
						fmt.Sprintf("Slot contention on %s (%s)", key, DepReadWrite))
					report.StateConflicts++
					report.HazardBreakdown[DepReadWrite]++
					s.recordHotspotEdgeLocked(key, st.lastWriter, tx.Hash, false)
				}
			}
			st.readers = append(st.readers, tx.Hash)
		}
	}
}

func (s *Scheduler) buildBalanceHazardsLocked(report *ConflictReport) {
	balStates := make(map[string]*slotState)

	// Clone tracked initial balances for running overdraft checks.
	simBalances := make(map[string]*big.Int, len(s.initialBalances))
	for addr, bal := range s.initialBalances {
		simBalances[addr] = new(big.Int).Set(bal)
	}

	for _, tx := range s.ordered {
		from := strings.ToLower(strings.TrimSpace(tx.From))
		to := strings.ToLower(strings.TrimSpace(tx.To))

		balReads := make(map[string]struct{})
		balWrites := make(map[string]struct{})
		overdraftAccts := make(map[string]bool)

		hasValue := tx.Value != nil && tx.Value.Sign() > 0
		hasGas := tx.GasPrice != nil && tx.GasPrice.Sign() > 0

		if from != "" && (hasValue || hasGas) {
			balReads[from] = struct{}{}
			balWrites[from] = struct{}{}
		}
		if to != "" && hasValue {
			balWrites[to] = struct{}{}
		}
		for _, acct := range tx.extraBalanceReads {
			if norm := strings.ToLower(strings.TrimSpace(acct)); norm != "" {
				balReads[norm] = struct{}{}
			}
		}
		for _, acct := range tx.extraBalanceWrites {
			if norm := strings.ToLower(strings.TrimSpace(acct)); norm != "" {
				balWrites[norm] = struct{}{}
			}
		}

		// Simulate tracked native balance transitions.
		if from != "" {
			if curBal, tracked := simBalances[from]; tracked {
				required := tx.MaxGasCost()
				if hasValue {
					required.Add(required, tx.Value)
				}
				if required.Sign() > 0 {
					if curBal.Cmp(required) < 0 {
						s.balanceViolations = append(s.balanceViolations, BalanceViolation{
							Account:  from,
							TxHash:   tx.Hash,
							Required: required.String(),
							Balance:  curBal.String(),
						})
						report.BalanceViolations++
						report.HazardBreakdown[DepBalanceDrain]++
						overdraftAccts[from] = true
					} else {
						curBal.Sub(curBal, required)
					}
				}
			}
		}
		if to != "" && hasValue {
			if curBal, tracked := simBalances[to]; tracked {
				curBal.Add(curBal, tx.Value)
			}
		}

		// Process explicit BalanceDelta entries in sorted order.
		if len(tx.BalanceDelta) > 0 {
			deltaKeys := make([]string, 0, len(tx.BalanceDelta))
			for acct := range tx.BalanceDelta {
				deltaKeys = append(deltaKeys, acct)
			}
			sort.Strings(deltaKeys)

			for _, rawAcct := range deltaKeys {
				delta := tx.BalanceDelta[rawAcct]
				if delta == nil || delta.Sign() == 0 {
					continue
				}
				acct := strings.ToLower(strings.TrimSpace(rawAcct))
				if acct == "" {
					continue
				}
				balWrites[acct] = struct{}{}
				if delta.Sign() < 0 {
					balReads[acct] = struct{}{}
				}

				if curBal, tracked := simBalances[acct]; tracked {
					nextBal := new(big.Int).Add(curBal, delta)
					if nextBal.Sign() < 0 {
						s.balanceViolations = append(s.balanceViolations, BalanceViolation{
							Account:  acct,
							TxHash:   tx.Hash,
							Required: new(big.Int).Abs(delta).String(),
							Balance:  curBal.String(),
						})
						report.BalanceViolations++
						report.HazardBreakdown[DepBalanceDrain]++
						overdraftAccts[acct] = true
					} else {
						simBalances[acct] = nextBal
					}
				}
			}
		}

		// Emit balance hazard edges across transactions touching the same account balance.
		writeAccts := sortedSetKeys(balWrites)
		for _, acct := range writeAccts {
			key := evm.BalanceKey(acct)
			s.recordHotspotTouchLocked(key, tx.Hash, true)
			if _, alsoReads := balReads[acct]; alsoReads {
				s.recordHotspotTouchLocked(key, tx.Hash, false)
			}
			st := balStates[key]
			if st == nil {
				balStates[key] = &slotState{lastWriter: tx.Hash}
				continue
			}

			depType := DepBalance
			if overdraftAccts[acct] {
				depType = DepBalanceDrain
			}

			if len(st.readers) > 0 {
				for _, reader := range st.readers {
					if reader == tx.Hash || s.hasEdgeLocked(reader, tx.Hash) {
						continue
					}
					s.addEdgeLocked(reader, tx.Hash, depType, key,
						fmt.Sprintf("Balance dependency on %s (%s)", acct, depType))
					report.BalanceHazards++
					if depType == DepBalance {
						report.HazardBreakdown[DepBalance]++
					}
					s.recordHotspotEdgeLocked(key, reader, tx.Hash, true)
				}
			} else if st.lastWriter != "" && st.lastWriter != tx.Hash && !s.hasEdgeLocked(st.lastWriter, tx.Hash) {
				s.addEdgeLocked(st.lastWriter, tx.Hash, depType, key,
					fmt.Sprintf("Balance dependency on %s (%s)", acct, depType))
				report.BalanceHazards++
				if depType == DepBalance {
					report.HazardBreakdown[DepBalance]++
				}
				s.recordHotspotEdgeLocked(key, st.lastWriter, tx.Hash, true)
			}

			st.lastWriter = tx.Hash
			st.readers = nil
		}

		readOnlyAccts := make([]string, 0, len(balReads))
		for acct := range balReads {
			if _, isWrite := balWrites[acct]; !isWrite {
				readOnlyAccts = append(readOnlyAccts, acct)
			}
		}
		sort.Strings(readOnlyAccts)

		for _, acct := range readOnlyAccts {
			key := evm.BalanceKey(acct)
			s.recordHotspotTouchLocked(key, tx.Hash, false)
			st := balStates[key]
			if st == nil {
				balStates[key] = &slotState{readers: []string{tx.Hash}}
				continue
			}
			if st.lastWriter != "" && st.lastWriter != tx.Hash && !s.hasEdgeLocked(st.lastWriter, tx.Hash) {
				s.addEdgeLocked(st.lastWriter, tx.Hash, DepBalance, key,
					fmt.Sprintf("Balance RAW dependency on %s", acct))
				report.BalanceHazards++
				report.HazardBreakdown[DepBalance]++
				s.recordHotspotEdgeLocked(key, st.lastWriter, tx.Hash, false)
			}
			st.readers = append(st.readers, tx.Hash)
		}
	}
}

func (s *Scheduler) populateMetricsLocked(report *ConflictReport, waves [][]*Transaction) {
	report.IndependentWaves = len(waves)
	maxConc := 0
	var waveBarrierGas uint64

	for _, w := range waves {
		if len(w) > maxConc {
			maxConc = len(w)
		}
		var maxGas uint64
		for _, tx := range w {
			if g := tx.EffectiveGasLimit(); g > maxGas {
				maxGas = g
			}
		}
		waveBarrierGas += maxGas
	}
	report.MaxConcurrency = maxConc

	if len(waves) > 0 {
		report.TheoreticalSpeedup = float64(len(s.ordered)) / float64(len(waves))
	}

	cp := s.computeCriticalPathLocked(waves)
	report.CriticalPathLen = cp.HopLength
	report.CriticalPathGas = cp.GasWeight

	if report.TotalGas > 0 && cp.GasWeight > 0 {
		report.GasWeightedSpeedup = float64(report.TotalGas) / float64(cp.GasWeight)
	}
	if report.TotalGas > 0 && waveBarrierGas > 0 {
		report.WaveGasSpeedup = float64(report.TotalGas) / float64(waveBarrierGas)
	}
	if maxConc > 0 && waveBarrierGas > 0 {
		report.ParallelEfficiency = float64(report.TotalGas) / (float64(waveBarrierGas) * float64(maxConc))
	}
}

func (s *Scheduler) addEdgeLocked(from, to string, depType DependencyType, key, detail string) {
	edge := &Edge{
		FromTx: from,
		ToTx:   to,
		Type:   depType,
		Key:    key,
		Detail: detail,
	}
	s.adj[from] = append(s.adj[from], edge)
	s.revAdj[to] = append(s.revAdj[to], edge)
	s.edges = append(s.edges, edge)
	s.inDegree[to]++
}

func (s *Scheduler) hasEdgeLocked(from, to string) bool {
	for _, e := range s.adj[from] {
		if e.ToTx == to {
			return true
		}
	}
	return false
}

func (s *Scheduler) recordHotspotTouchLocked(key, txHash string, isWrite bool) {
	if key == "" {
		return
	}
	acc := s.hotspots[key]
	if acc == nil {
		acc = &hotspotAcc{txSet: make(map[string]struct{})}
		s.hotspots[key] = acc
	}
	if isWrite {
		acc.writes++
	} else {
		acc.reads++
	}
	if _, seen := acc.txSet[txHash]; !seen {
		acc.txSet[txHash] = struct{}{}
		acc.txs = append(acc.txs, txHash)
	}
}

func (s *Scheduler) recordHotspotEdgeLocked(key, fromHash, toHash string, isWrite bool) {
	if key == "" {
		return
	}
	acc := s.hotspots[key]
	if acc == nil {
		acc = &hotspotAcc{txSet: make(map[string]struct{})}
		s.hotspots[key] = acc
	}
	acc.edges++
	for _, h := range []string{fromHash, toHash} {
		if _, seen := acc.txSet[h]; !seen {
			acc.txSet[h] = struct{}{}
			acc.txs = append(acc.txs, h)
		}
	}
	if acc.writes == 0 && isWrite {
		acc.writes = 1
	}
}

// ComputeParallelWaves partitions transactions into sequentially dependent waves of parallel tasks.
func (s *Scheduler) ComputeParallelWaves() ([][]*Transaction, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.computeParallelWavesLocked()
}

func (s *Scheduler) computeParallelWavesLocked() ([][]*Transaction, error) {
	pool := s.ordered
	if len(pool) == 0 {
		pool = s.transactions
	}
	if len(pool) == 0 {
		return nil, nil
	}

	inDeg := make(map[string]int, len(pool))
	for _, tx := range pool {
		inDeg[tx.Hash] = s.inDegree[tx.Hash]
	}

	var waves [][]*Transaction
	processed := 0

	// Frontier queue avoids O(N * waves) full scans on large graphs.
	frontier := make([]*Transaction, 0, len(pool))
	for _, tx := range pool {
		if inDeg[tx.Hash] == 0 {
			frontier = append(frontier, tx)
		}
	}

	for len(frontier) > 0 {
		currentWave := frontier
		nextFrontier := make([]*Transaction, 0)

		for _, tx := range currentWave {
			inDeg[tx.Hash] = -1
			processed++
		}

		for _, tx := range currentWave {
			for _, edge := range s.adj[tx.Hash] {
				if inDeg[edge.ToTx] > 0 {
					inDeg[edge.ToTx]--
					if inDeg[edge.ToTx] == 0 {
						nextFrontier = append(nextFrontier, s.txMap[edge.ToTx])
					}
				}
			}
		}

		// Keep next frontier deterministically ordered by canonical sequence index.
		sort.Slice(nextFrontier, func(i, j int) bool {
			return s.orderIndex[nextFrontier[i].Hash] < s.orderIndex[nextFrontier[j].Hash]
		})

		waves = append(waves, currentWave)
		frontier = nextFrontier
	}

	if processed < len(pool) {
		return nil, ErrCyclicDependency
	}

	return waves, nil
}

// TopologicalSort returns a valid sequential execution ordering.
func (s *Scheduler) TopologicalSort() ([]*Transaction, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	waves, err := s.computeParallelWavesLocked()
	if err != nil {
		return nil, err
	}

	sorted := make([]*Transaction, 0, len(s.transactions))
	for _, wave := range waves {
		sorted = append(sorted, wave...)
	}
	return sorted, nil
}

// ComputeCriticalPath returns both hop-based and gas-weighted critical paths across the DAG.
func (s *Scheduler) ComputeCriticalPath() (CriticalPath, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	waves, err := s.computeParallelWavesLocked()
	if err != nil {
		return CriticalPath{}, err
	}
	return s.computeCriticalPathLocked(waves), nil
}

func (s *Scheduler) computeCriticalPathLocked(waves [][]*Transaction) CriticalPath {
	if len(waves) == 0 {
		return CriticalPath{}
	}

	topo := make([]*Transaction, 0, len(s.ordered))
	for _, w := range waves {
		topo = append(topo, w...)
	}

	hopDist := make(map[string]int, len(topo))
	hopPrev := make(map[string]string, len(topo))
	gasDist := make(map[string]uint64, len(topo))
	gasPrev := make(map[string]string, len(topo))
	gasEdgeKey := make(map[string]string, len(topo))

	for _, tx := range topo {
		hopDist[tx.Hash] = 1
		gasDist[tx.Hash] = tx.EffectiveGasLimit()
	}

	for _, u := range topo {
		uh := u.Hash
		uHop := hopDist[uh]
		uGas := gasDist[uh]

		for _, edge := range s.adj[uh] {
			vh := edge.ToTx
			vTx := s.txMap[vh]
			if vTx == nil {
				continue
			}

			if candHop := uHop + 1; candHop > hopDist[vh] {
				hopDist[vh] = candHop
				hopPrev[vh] = uh
			}
			if candGas := uGas + vTx.EffectiveGasLimit(); candGas > gasDist[vh] {
				gasDist[vh] = candGas
				gasPrev[vh] = uh
				gasEdgeKey[vh] = edge.Key
			}
		}
	}

	bestHopEnd := topo[0].Hash
	bestGasEnd := topo[0].Hash
	for _, tx := range topo {
		if hopDist[tx.Hash] > hopDist[bestHopEnd] {
			bestHopEnd = tx.Hash
		}
		if gasDist[tx.Hash] > gasDist[bestGasEnd] {
			bestGasEnd = tx.Hash
		}
	}

	hopChain := backtrackChain(bestHopEnd, hopPrev)
	gasChain := backtrackChain(bestGasEnd, gasPrev)

	keySeen := make(map[string]struct{})
	bottleneckKeys := make([]string, 0)
	for _, h := range gasChain {
		if k := gasEdgeKey[h]; k != "" {
			if _, ok := keySeen[k]; !ok {
				keySeen[k] = struct{}{}
				bottleneckKeys = append(bottleneckKeys, k)
			}
		}
	}

	return CriticalPath{
		HopLength:      hopDist[bestHopEnd],
		HopChain:       hopChain,
		GasWeight:      gasDist[bestGasEnd],
		GasChain:       gasChain,
		BottleneckKeys: bottleneckKeys,
	}
}

func backtrackChain(end string, prev map[string]string) []string {
	var rev []string
	for cur := end; cur != ""; cur = prev[cur] {
		rev = append(rev, cur)
	}
	for i, j := 0, len(rev)-1; i < j; i, j = i+1, j-1 {
		rev[i], rev[j] = rev[j], rev[i]
	}
	return rev
}

// ComputeWaveMetrics evaluates per-wave gas totals, straggler latency, and load imbalance.
func (s *Scheduler) ComputeWaveMetrics() ([]WaveMetrics, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	waves, err := s.computeParallelWavesLocked()
	if err != nil {
		return nil, err
	}
	return s.computeWaveMetricsLocked(waves), nil
}

func (s *Scheduler) computeWaveMetricsLocked(waves [][]*Transaction) []WaveMetrics {
	metrics := make([]WaveMetrics, len(waves))
	for i, w := range waves {
		var totalGas, maxGas uint64
		hashes := make([]string, len(w))
		for j, tx := range w {
			g := tx.EffectiveGasLimit()
			totalGas += g
			if g > maxGas {
				maxGas = g
			}
			hashes[j] = tx.Hash
		}
		var avgGas, imbalance float64
		if len(w) > 0 {
			avgGas = float64(totalGas) / float64(len(w))
			if avgGas > 0 {
				imbalance = float64(maxGas) / avgGas
			}
		}
		metrics[i] = WaveMetrics{
			WaveIndex:     i,
			TxCount:       len(w),
			TotalGas:      totalGas,
			MaxTxGas:      maxGas,
			AvgTxGas:      avgGas,
			LoadImbalance: imbalance,
			Transactions:  hashes,
		}
	}
	return metrics
}

// SimulateWorkers simulates parallel block execution on a fixed pool of numWorkers cores,
// comparing synchronous wave-barrier dispatch against critical-path work-stealing DAG dispatch.
func (s *Scheduler) SimulateWorkers(numWorkers int) (WorkerSimulation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	waves, err := s.computeParallelWavesLocked()
	if err != nil {
		return WorkerSimulation{}, err
	}
	return s.simulateWorkersLocked(waves, numWorkers), nil
}

func (s *Scheduler) simulateWorkersLocked(waves [][]*Transaction, numWorkers int) WorkerSimulation {
	if numWorkers < 1 {
		numWorkers = 1
	}
	if len(waves) == 0 {
		return WorkerSimulation{Workers: numWorkers}
	}

	topo := make([]*Transaction, 0, len(s.ordered))
	var seqGas uint64
	for _, w := range waves {
		for _, tx := range w {
			topo = append(topo, tx)
			seqGas += tx.EffectiveGasLimit()
		}
	}

	// 1. Synchronous wave-barrier execution using Longest Processing Time (LPT) per wave.
	var waveBarrierGas uint64
	loads := make([]uint64, numWorkers)
	for _, w := range waves {
		for i := range loads {
			loads[i] = 0
		}
		gasList := make([]uint64, len(w))
		for i, tx := range w {
			gasList[i] = tx.EffectiveGasLimit()
		}
		sort.Slice(gasList, func(i, j int) bool {
			return gasList[i] > gasList[j]
		})
		for _, g := range gasList {
			minIdx := 0
			for k := 1; k < numWorkers; k++ {
				if loads[k] < loads[minIdx] {
					minIdx = k
				}
			}
			loads[minIdx] += g
		}
		var waveMax uint64
		for _, l := range loads {
			if l > waveMax {
				waveMax = l
			}
		}
		waveBarrierGas += waveMax
	}

	// 2. Dynamic work-stealing DAG dispatch prioritized by remaining critical-path gas.
	remCritGas := make(map[string]uint64, len(topo))
	for i := len(topo) - 1; i >= 0; i-- {
		tx := topo[i]
		var maxChild uint64
		for _, e := range s.adj[tx.Hash] {
			if remCritGas[e.ToTx] > maxChild {
				maxChild = remCritGas[e.ToTx]
			}
		}
		remCritGas[tx.Hash] = tx.EffectiveGasLimit() + maxChild
	}

	inDeg := make(map[string]int, len(topo))
	earliestStart := make(map[string]uint64, len(topo))
	ready := make([]*Transaction, 0, len(topo))
	for _, tx := range topo {
		deg := s.inDegree[tx.Hash]
		inDeg[tx.Hash] = deg
		if deg == 0 {
			ready = append(ready, tx)
		}
	}

	workerFreeAt := make([]uint64, numWorkers)
	scheduled := 0

	for scheduled < len(topo) {
		minWorkerIdx := 0
		for k := 1; k < numWorkers; k++ {
			if workerFreeAt[k] < workerFreeAt[minWorkerIdx] {
				minWorkerIdx = k
			}
		}
		clock := workerFreeAt[minWorkerIdx]

		// Determine if any ready transaction is eligible at current worker clock;
		// if not, advance clock to the earliest ready transaction start time.
		hasEligible := false
		var minReadyStart uint64
		for i, tx := range ready {
			es := earliestStart[tx.Hash]
			if i == 0 || es < minReadyStart {
				minReadyStart = es
			}
			if es <= clock {
				hasEligible = true
			}
		}
		if !hasEligible {
			clock = minReadyStart
		}

		// Pick the eligible ready transaction with maximum remaining critical-path gas.
		bestIdx := -1
		for i, tx := range ready {
			if earliestStart[tx.Hash] > clock {
				continue
			}
			if bestIdx == -1 {
				bestIdx = i
				continue
			}
			curRem := remCritGas[tx.Hash]
			bestRem := remCritGas[ready[bestIdx].Hash]
			if curRem > bestRem || (curRem == bestRem && s.orderIndex[tx.Hash] < s.orderIndex[ready[bestIdx].Hash]) {
				bestIdx = i
			}
		}

		chosen := ready[bestIdx]
		// Remove chosen from ready slice.
		ready[bestIdx] = ready[len(ready)-1]
		ready = ready[:len(ready)-1]

		start := workerFreeAt[minWorkerIdx]
		if earliestStart[chosen.Hash] > start {
			start = earliestStart[chosen.Hash]
		}
		finish := start + chosen.EffectiveGasLimit()
		workerFreeAt[minWorkerIdx] = finish
		scheduled++

		for _, edge := range s.adj[chosen.Hash] {
			if finish > earliestStart[edge.ToTx] {
				earliestStart[edge.ToTx] = finish
			}
			inDeg[edge.ToTx]--
			if inDeg[edge.ToTx] == 0 {
				ready = append(ready, s.txMap[edge.ToTx])
			}
		}
	}

	var workStealingGas uint64
	for _, t := range workerFreeAt {
		if t > workStealingGas {
			workStealingGas = t
		}
	}

	sim := WorkerSimulation{
		Workers:         numWorkers,
		SequentialGas:   seqGas,
		WaveBarrierGas:  waveBarrierGas,
		WorkStealingGas: workStealingGas,
	}
	if waveBarrierGas > 0 {
		sim.WaveBarrierSpeedup = float64(seqGas) / float64(waveBarrierGas)
		sim.WaveUtilization = float64(seqGas) / (float64(waveBarrierGas) * float64(numWorkers))
	}
	if workStealingGas > 0 {
		sim.WorkStealingSpeedup = float64(seqGas) / float64(workStealingGas)
		sim.WorkStealUtilization = float64(seqGas) / (float64(workStealingGas) * float64(numWorkers))
	}
	return sim
}

// ComputeHotspots returns the top contended storage slots and accounts ranked by dependency edge count.
func (s *Scheduler) ComputeHotspots(limit int) []ContentionHotspot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.computeHotspotsLocked(limit)
}

func (s *Scheduler) computeHotspotsLocked(limit int) []ContentionHotspot {
	list := make([]ContentionHotspot, 0, len(s.hotspots))
	for key, acc := range s.hotspots {
		if acc.edges == 0 {
			continue
		}
		txsCopy := make([]string, len(acc.txs))
		copy(txsCopy, acc.txs)
		list = append(list, ContentionHotspot{
			Key:        key,
			WriteCount: acc.writes,
			ReadCount:  acc.reads,
			EdgeCount:  acc.edges,
			TxHashes:   txsCopy,
		})
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].EdgeCount != list[j].EdgeCount {
			return list[i].EdgeCount > list[j].EdgeCount
		}
		if list[i].WriteCount != list[j].WriteCount {
			return list[i].WriteCount > list[j].WriteCount
		}
		return list[i].Key < list[j].Key
	})
	if limit > 0 && len(list) > limit {
		list = list[:limit]
	}
	return list
}

// Edges returns a snapshot of all directed dependency edges in the constructed graph.
func (s *Scheduler) Edges() []*Edge {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Edge, len(s.edges))
	for i, e := range s.edges {
		cp := *e
		out[i] = &cp
	}
	return out
}

// NonceAnomalies returns detected duplicate-nonce conflicts and nonce sequence gaps.
func (s *Scheduler) NonceAnomalies() []NonceAnomaly {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]NonceAnomaly, len(s.nonceAnomalies))
	copy(out, s.nonceAnomalies)
	return out
}

// BalanceViolations returns detected account overdraft / balance drain violations.
func (s *Scheduler) BalanceViolations() []BalanceViolation {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]BalanceViolation, len(s.balanceViolations))
	copy(out, s.balanceViolations)
	return out
}

// Analyze builds the graph (if needed) and returns a complete ScheduleAnalysis report
// including critical paths, per-wave metrics, worker simulation, hotspots, and anomalies.
func (s *Scheduler) Analyze(numWorkers, hotspotLimit int) (*ScheduleAnalysis, error) {
	report := s.BuildGraph()
	s.mu.RLock()
	defer s.mu.RUnlock()

	waves, err := s.computeParallelWavesLocked()
	if err != nil {
		return &ScheduleAnalysis{
			Report:            report,
			NonceAnomalies:    append([]NonceAnomaly(nil), s.nonceAnomalies...),
			BalanceViolations: append([]BalanceViolation(nil), s.balanceViolations...),
		}, err
	}

	return &ScheduleAnalysis{
		Report:            report,
		CriticalPath:      s.computeCriticalPathLocked(waves),
		WaveMetrics:       s.computeWaveMetricsLocked(waves),
		WorkerSimulation:  s.simulateWorkersLocked(waves, numWorkers),
		Hotspots:          s.computeHotspotsLocked(hotspotLimit),
		NonceAnomalies:    append([]NonceAnomaly(nil), s.nonceAnomalies...),
		BalanceViolations: append([]BalanceViolation(nil), s.balanceViolations...),
	}, nil
}

func extractSlotSets(tx *Transaction) (map[string]struct{}, map[string]struct{}) {
	readSet := make(map[string]struct{})
	writeSet := make(map[string]struct{})

	for contract, slots := range tx.ReadSlots {
		addr := strings.ToLower(strings.TrimSpace(contract))
		for _, slot := range slots {
			key := evm.StorageKey{Address: addr, Slot: slot}.String()
			readSet[key] = struct{}{}
		}
	}
	for contract, slots := range tx.WriteSlots {
		addr := strings.ToLower(strings.TrimSpace(contract))
		for _, slot := range slots {
			key := evm.StorageKey{Address: addr, Slot: slot}.String()
			writeSet[key] = struct{}{}
		}
	}
	for _, tuple := range tx.AccessList {
		addr := strings.ToLower(strings.TrimSpace(tuple.Address))
		if addr == "" {
			continue
		}
		if len(tuple.StorageKeys) == 0 {
			readSet[evm.StorageKey{Address: addr, Slot: "account_meta"}.String()] = struct{}{}
			continue
		}
		for _, slot := range tuple.StorageKeys {
			key := evm.StorageKey{Address: addr, Slot: slot}.String()
			readSet[key] = struct{}{}
			writeSet[key] = struct{}{}
		}
	}

	return readSet, writeSet
}

func sortedSetKeys(m map[string]struct{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
