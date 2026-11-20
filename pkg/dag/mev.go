package dag

import (
	"fmt"
	"math/big"
	"sort"
	"strings"

	"github.com/Aetherx86/Scapolite/pkg/evm"
)

// MEVThreatType classifies a detected mempool exploitation or state contention pattern.
type MEVThreatType string

const (
	MEVSandwich   MEVThreatType = "SANDWICH_ATTACK"
	MEVFrontrun   MEVThreatType = "FRONTRUNNING"
	MEVBackrun    MEVThreatType = "BACKRUNNING"
	MEVContention MEVThreatType = "HIGH_CONTENTION_HOTSPOT"
)

// MEVAlert encapsulates details of a detected adversarial ordering or hotspot pattern.
type MEVAlert struct {
	Type          MEVThreatType `json:"type"`
	Severity      string        `json:"severity"` // "HIGH", "MEDIUM", "LOW"
	TxHashes      []string      `json:"tx_hashes"`
	ConflictedKey string        `json:"conflicted_key"`
	Description   string        `json:"description"`
}

// MEVDetector inspects a ConflictDAG and transaction fee gradients for MEV heuristics.
type MEVDetector struct {
	DAG              *ConflictDAG
	HotspotThreshold int
}

// NewMEVDetector initializes an MEV threat detector for a constructed ConflictDAG.
func NewMEVDetector(dag *ConflictDAG) *MEVDetector {
	return &MEVDetector{
		DAG:              dag,
		HotspotThreshold: 4,
	}
}

// DetectThreats scans shared state write sequences and gas tip differentials for
// sandwich attacks, priority-gas frontrunning, gas-matched backrunning, and hotspots.
func (md *MEVDetector) DetectThreats() []MEVAlert {
	if md == nil || md.DAG == nil || len(md.DAG.Order) == 0 {
		return nil
	}

	threshold := md.HotspotThreshold
	if threshold <= 1 {
		threshold = 4
	}

	// Build a deterministic slot -> ordered writers index (excluding native balance keys)
	// so pattern scans scale with per-slot contention rather than O(N^3) over the full block.
	slotWriters := make(map[string][]*evm.Transaction)
	for _, hash := range md.DAG.Order {
		tx := md.DAG.Transactions[hash]
		if tx == nil {
			continue
		}
		for key := range tx.WriteSet {
			if evm.IsBalanceKey(key) {
				continue
			}
			slotWriters[key] = append(slotWriters[key], tx)
		}
	}

	keys := make([]string, 0, len(slotWriters))
	for k := range slotWriters {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var alerts []MEVAlert
	sandwichPairs := make(map[string]struct{})

	// 1. Sandwich Attack Detection:
	// Pattern: TxA (Attacker frontrun) -> TxB (Victim) -> TxC (Attacker backrun)
	// on the same written contract/AMM slot with TxA.tip >= TxB.tip.
	seenSandwich := make(map[string]struct{})
	for _, key := range keys {
		writers := slotWriters[key]
		if len(writers) < 3 {
			continue
		}
		for i := 0; i < len(writers)-2; i++ {
			txA := writers[i]
			tipA := txA.EffectiveGasTip()
			for j := i + 1; j < len(writers)-1; j++ {
				txB := writers[j]
				if txA.From == txB.From {
					continue
				}
				tipB := txB.EffectiveGasTip()
				if tipA.Cmp(tipB) < 0 {
					continue
				}

				for k := j + 1; k < len(writers); k++ {
					txC := writers[k]
					if txC.From != txA.From {
						continue
					}
					sig := txA.Hash + "|" + txB.Hash + "|" + txC.Hash
					if _, dup := seenSandwich[sig]; dup {
						break
					}
					seenSandwich[sig] = struct{}{}
					sandwichPairs[txB.Hash+"|"+txC.Hash] = struct{}{}

					alerts = append(alerts, MEVAlert{
						Type:          MEVSandwich,
						Severity:      "HIGH",
						TxHashes:      []string{txA.Hash, txB.Hash, txC.Hash},
						ConflictedKey: key,
						Description: fmt.Sprintf("Sandwich attack detected on %s: attacker (%s) front-ran victim (%s) with tip %s vs %s, then back-ran with %s",
							key, shortAddr(txA.From), shortAddr(txB.From), tipA.String(), tipB.String(), shortHash(txC.Hash)),
					})
					break // Match closest backrun leg per (frontrun, victim)
				}
			}
		}
	}

	// 2. Frontrunning / Priority Gas Auction (PGA) Detection:
	// Direct consecutive distinct-sender writers on the same state slot where the
	// preceding transaction outbid the successor by >= 20% effective gas tip.
	seenFrontrun := make(map[string]struct{})
	for _, key := range keys {
		writers := slotWriters[key]
		if len(writers) < 2 {
			continue
		}
		for i := 0; i < len(writers)-1; i++ {
			txA := writers[i]
			txB := writers[i+1]
			if txA.From == txB.From {
				continue
			}
			tipA := txA.EffectiveGasTip()
			tipB := txB.EffectiveGasTip()
			if tipA.Sign() <= 0 || tipB.Sign() <= 0 || tipA.Cmp(tipB) <= 0 {
				continue
			}

			diff := new(big.Int).Sub(tipA, tipB)
			ratio := new(big.Int).Mul(diff, big.NewInt(100))
			ratio.Div(ratio, tipB)
			if ratio.Int64() >= 20 {
				pairKey := txA.Hash + "|" + txB.Hash
				if _, dup := seenFrontrun[pairKey]; dup {
					continue
				}
				seenFrontrun[pairKey] = struct{}{}

				alerts = append(alerts, MEVAlert{
					Type:          MEVFrontrun,
					Severity:      "MEDIUM",
					TxHashes:      []string{txA.Hash, txB.Hash},
					ConflictedKey: key,
					Description: fmt.Sprintf("Priority gas frontrunning on %s: %s (tip: %s) outbid %s (tip: %s) by %d%%",
						key, shortHash(txA.Hash), tipA.String(), shortHash(txB.Hash), tipB.String(), ratio.Int64()),
				})
			}
		}
	}

	// 3. Arbitrage / Liquidation Backrunning Detection:
	// Consecutive distinct-sender write on an AMM or shared state slot where the follower
	// closely matches the target's effective tip (0% to 5% below or equal tip).
	seenBackrun := make(map[string]struct{})
	for _, key := range keys {
		writers := slotWriters[key]
		if len(writers) < 2 {
			continue
		}
		isAMMSlot := strings.Contains(key, ":amm:")
		for i := 0; i < len(writers)-1; i++ {
			txA := writers[i]
			txB := writers[i+1]
			if txA.From == txB.From {
				continue
			}
			if !isAMMSlot && txA.Action != "amm_swap" && txA.Action != "amm_liquidity" {
				continue
			}
			pairKey := txA.Hash + "|" + txB.Hash
			if _, inSandwich := sandwichPairs[pairKey]; inSandwich {
				continue
			}
			if _, dup := seenBackrun[pairKey]; dup {
				continue
			}

			tipA := txA.EffectiveGasTip()
			tipB := txB.EffectiveGasTip()
			if tipA.Sign() <= 0 || tipB.Sign() <= 0 || tipB.Cmp(tipA) > 0 {
				continue
			}

			// Tip difference within 5% of target tip indicates intentional backrun positioning.
			diff := new(big.Int).Sub(tipA, tipB)
			diffPct := new(big.Int).Mul(diff, big.NewInt(100))
			diffPct.Div(diffPct, tipA)
			if diffPct.Int64() <= 5 {
				seenBackrun[pairKey] = struct{}{}
				alerts = append(alerts, MEVAlert{
					Type:          MEVBackrun,
					Severity:      "MEDIUM",
					TxHashes:      []string{txA.Hash, txB.Hash},
					ConflictedKey: key,
					Description: fmt.Sprintf("Backrun positioning on %s: %s (%s, tip: %s) shadowed %s (tip: %s) within %d%%",
						key, shortHash(txB.Hash), shortAddr(txB.From), tipB.String(), shortHash(txA.Hash), tipA.String(), diffPct.Int64()),
				})
			}
		}
	}

	// 4. High-Contention Storage Hotspot Detection.
	for _, key := range keys {
		writers := slotWriters[key]
		if len(writers) >= threshold {
			hashes := make([]string, len(writers))
			for i, tx := range writers {
				hashes[i] = tx.Hash
			}
			alerts = append(alerts, MEVAlert{
				Type:          MEVContention,
				Severity:      "LOW",
				TxHashes:      hashes,
				ConflictedKey: key,
				Description: fmt.Sprintf("State hotspot on %s: %d transactions contending for write access (serial execution bottleneck)",
					key, len(writers)),
			})
		}
	}

	sort.Slice(alerts, func(i, j int) bool {
		si, sj := severityRank(alerts[i].Severity), severityRank(alerts[j].Severity)
		if si != sj {
			return si < sj
		}
		if alerts[i].Type != alerts[j].Type {
			return alerts[i].Type < alerts[j].Type
		}
		if alerts[i].ConflictedKey != alerts[j].ConflictedKey {
			return alerts[i].ConflictedKey < alerts[j].ConflictedKey
		}
		if len(alerts[i].TxHashes) > 0 && len(alerts[j].TxHashes) > 0 {
			return alerts[i].TxHashes[0] < alerts[j].TxHashes[0]
		}
		return len(alerts[i].TxHashes) < len(alerts[j].TxHashes)
	})

	return alerts
}

func severityRank(s string) int {
	switch s {
	case "HIGH":
		return 0
	case "MEDIUM":
		return 1
	case "LOW":
		return 2
	default:
		return 3
	}
}

func shortAddr(a string) string {
	if len(a) <= 10 {
		return a
	}
	if strings.HasPrefix(a, "0x") {
		return a[:6] + ".." + a[len(a)-4:]
	}
	return a[:4] + ".." + a[len(a)-4:]
}
