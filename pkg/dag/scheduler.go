package dag

import (
	"errors"
	"fmt"
	"sort"
	"sync"
)

var (
	ErrCyclicDependency = errors.New("cyclic dependency detected in transaction graph")
	ErrTransactionNotFound = errors.New("transaction not found in graph")
)

// Scheduler builds and schedules concurrent EVM transaction execution graphs.
type Scheduler struct {
	mu           sync.RWMutex
	transactions []*Transaction
	txMap        map[string]*Transaction
	adj          map[string][]*Edge
	revAdj       map[string][]*Edge
	inDegree     map[string]int
	mode         string // "fast" or "deep"
}

// NewScheduler creates a transaction dependency scheduler.
func NewScheduler(mode string) *Scheduler {
	if mode == "" {
		mode = "fast"
	}
	return &Scheduler{
		txMap:    make(map[string]*Transaction),
		adj:      make(map[string][]*Edge),
		revAdj:   make(map[string][]*Edge),
		inDegree: make(map[string]int),
		mode:     mode,
	}
}

// AddTransaction appends a transaction to the scheduling pool.
func (s *Scheduler) AddTransaction(tx *Transaction) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.transactions = append(s.transactions, tx)
	s.txMap[tx.Hash] = tx
	s.inDegree[tx.Hash] = 0
}

// BuildGraph constructs dependency edges between all ingested transactions.
func (s *Scheduler) BuildGraph() *ConflictReport {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Reset graph state
	s.adj = make(map[string][]*Edge)
	s.revAdj = make(map[string][]*Edge)
	for _, tx := range s.transactions {
		s.inDegree[tx.Hash] = 0
	}

	report := &ConflictReport{
		TotalTransactions: len(s.transactions),
	}

	// 1. Group by sender address to enforce sequential Nonce dependencies
	bySender := make(map[string][]*Transaction)
	for _, tx := range s.transactions {
		bySender[tx.From] = append(bySender[tx.From], tx)
	}

	for _, txs := range bySender {
		if len(txs) > 1 {
			sort.Slice(txs, func(i, j int) bool {
				return txs[i].Nonce < txs[j].Nonce
			})
			for i := 0; i < len(txs)-1; i++ {
				s.addEdge(txs[i].Hash, txs[i+1].Hash, DepNonce,
					fmt.Sprintf("Nonce sequence: %s nonce %d -> %d", txs[i].From, txs[i].Nonce, txs[i+1].Nonce))
				report.NonceDependencies++
			}
		}
	}

	// 2. State Access Conflicts (Fine-grained or Coarse-grained based on mode)
	if s.mode == "fast" {
		// Fast Mode: Coarse-grained contract and recipient address contention
		contractAccessors := make(map[string][]string) // targetAddr -> []txHash
		for _, tx := range s.transactions {
			if tx.To != "" {
				contractAccessors[tx.To] = append(contractAccessors[tx.To], tx.Hash)
			}
		}

		for addr, hashes := range contractAccessors {
			if len(hashes) > 1 {
				for i := 0; i < len(hashes)-1; i++ {
					// Avoid redundant edge if already connected
					if !s.hasEdge(hashes[i], hashes[i+1]) {
						s.addEdge(hashes[i], hashes[i+1], DepWriteWrite,
							fmt.Sprintf("Coarse contract address conflict on %s", addr))
						report.StateConflicts++
					}
				}
			}
		}
	} else {
		// Deep Mode: Fine-grained storage slot (SSTORE / SLOAD) footprint tracking
		type slotAccess struct {
			txHash  string
			isWrite bool
		}
		slotMap := make(map[string][]slotAccess) // "contractAddr:slotKey" -> accesses

		for _, tx := range s.transactions {
			// Track Reads
			for contract, slots := range tx.ReadSlots {
				for _, slot := range slots {
					key := fmt.Sprintf("%s:%s", contract, slot)
					slotMap[key] = append(slotMap[key], slotAccess{txHash: tx.Hash, isWrite: false})
				}
			}
			// Track Writes
			for contract, slots := range tx.WriteSlots {
				for _, slot := range slots {
					key := fmt.Sprintf("%s:%s", contract, slot)
					slotMap[key] = append(slotMap[key], slotAccess{txHash: tx.Hash, isWrite: true})
				}
			}
		}

		for key, accesses := range slotMap {
			if len(accesses) > 1 {
				for i := 0; i < len(accesses); i++ {
					for j := i + 1; j < len(accesses); j++ {
						a1 := accesses[i]
						a2 := accesses[j]
						if a1.txHash == a2.txHash {
							continue
						}

						// Conflict if at least one is a write
						if a1.isWrite || a2.isWrite {
							depType := DepReadWrite
							if a1.isWrite && !a2.isWrite {
								depType = DepReadWrite
							} else if !a1.isWrite && a2.isWrite {
								depType = DepWriteRead
							} else {
								depType = DepWriteWrite
							}

							if !s.hasEdge(a1.txHash, a2.txHash) {
								s.addEdge(a1.txHash, a2.txHash, depType,
									fmt.Sprintf("Slot contention on %s (%s)", key, depType))
								report.StateConflicts++
							}
						}
					}
				}
			}
		}
	}

	// Schedule waves and calculate concurrency metrics
	waves, err := s.ComputeParallelWaves()
	if err != nil {
		report.CyclesDetected = true
	} else {
		report.IndependentWaves = len(waves)
		maxConc := 0
		for _, w := range waves {
			if len(w) > maxConc {
				maxConc = len(w)
			}
		}
		report.MaxConcurrency = maxConc
		if len(waves) > 0 {
			report.TheoreticalSpeedup = float64(len(s.transactions)) / float64(len(waves))
		}
	}

	return report
}

func (s *Scheduler) addEdge(from, to string, depType DependencyType, detail string) {
	edge := &Edge{
		FromTx: from,
		ToTx:   to,
		Type:   depType,
		Detail: detail,
	}
	s.adj[from] = append(s.adj[from], edge)
	s.revAdj[to] = append(s.revAdj[to], edge)
	s.inDegree[to]++
}

func (s *Scheduler) hasEdge(from, to string) bool {
	for _, e := range s.adj[from] {
		if e.ToTx == to {
			return true
		}
	}
	return false
}

// ComputeParallelWaves partitions transactions into sequentially dependent waves of parallel tasks.
func (s *Scheduler) ComputeParallelWaves() ([][]*Transaction, error) {
	// Copy in-degrees for Kahn's algorithm
	inDeg := make(map[string]int, len(s.inDegree))
	for k, v := range s.inDegree {
		inDeg[k] = v
	}

	var waves [][]*Transaction
	processed := 0

	for {
		// Identify all transactions with in-degree 0
		var currentWave []*Transaction
		for _, tx := range s.transactions {
			if inDeg[tx.Hash] == 0 {
				currentWave = append(currentWave, tx)
			}
		}

		if len(currentWave) == 0 {
			break
		}

		// Mark current wave as scheduled
		for _, tx := range currentWave {
			inDeg[tx.Hash] = -1 // remove from pool
			processed++
		}

		// Decrement in-degrees of successors
		for _, tx := range currentWave {
			for _, edge := range s.adj[tx.Hash] {
				if inDeg[edge.ToTx] > 0 {
					inDeg[edge.ToTx]--
				}
			}
		}

		waves = append(waves, currentWave)
	}

	if processed < len(s.transactions) {
		return nil, ErrCyclicDependency
	}

	return waves, nil
}

// TopologicalSort returns a valid sequential execution ordering.
func (s *Scheduler) TopologicalSort() ([]*Transaction, error) {
	waves, err := s.ComputeParallelWaves()
	if err != nil {
		return nil, err
	}

	var sorted []*Transaction
	for _, wave := range waves {
		sorted = append(sorted, wave...)
	}
	return sorted, nil
}
