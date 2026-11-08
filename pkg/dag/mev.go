package dag

import (
	"fmt"
	"math/big"
	"strings"
)

// MEVThreatType classifies the specific mempool exploitation vector.
type MEVThreatType string

const (
	MEVSandwich    MEVThreatType = "SANDWICH_ATTACK"
	MEVFrontrun    MEVThreatType = "FRONTRUNNING"
	MEVBackrun     MEVThreatType = "BACKRUNNING"
	MEVContention  MEVThreatType = "HIGH_CONTENTION_HOTSPOT"
)

// MEVAlert encapsulates details of a detected conflict or adversarial order manipulation.
type MEVAlert struct {
	Type        MEVThreatType `json:"type"`
	Severity    string        `json:"severity"` // "HIGH", "MEDIUM", "LOW"
	TxHashes    []string      `json:"tx_hashes"`
	ConflictedKey string      `json:"conflicted_key"`
	Description string        `json:"description"`
}

// MEVDetector inspects the conflict DAG and mempool transaction properties.
type MEVDetector struct {
	DAG *ConflictDAG
}

// NewMEVDetector initializes an auditor for a given DAG.
func NewMEVDetector(dag *ConflictDAG) *MEVDetector {
	return &MEVDetector{DAG: dag}
}

// DetectThreats scans transaction sequences, gas tip differentials, and shared AMM storage keys.
func (md *MEVDetector) DetectThreats() []MEVAlert {
	var alerts []MEVAlert

	// 1. Sandwich Attack Detection:
	// Pattern: TxA (Frontrun Buy) -> TxB (Victim Trade) -> TxC (Backrun Sell)
	// TxA and TxC originate from the same attacker address or bundle searcher,
	// targeting the exact same AMM state key as TxB.
	// Gas tip: TxA.tip >= TxB.tip >= TxC.tip
	alerts = append(alerts, md.detectSandwiches()...)

	// 2. Frontrunning / Priority Gas Auction (PGA) Detection:
	// Two distinct senders touching the identical contract/state slot with significant gas tip disparity
	alerts = append(alerts, md.detectFrontrunning()...)

	// 3. High Contention Storage Hotspot:
	// More than N transactions attempting to write to the identical state key in one block
	alerts = append(alerts, md.detectHotspots(4)...)

	return alerts
}

func (md *MEVDetector) detectSandwiches() []MEVAlert {
	var alerts []MEVAlert
	order := md.DAG.Order

	if len(order) < 3 {
		return alerts
	}

	for i := 0; i < len(order)-2; i++ {
		txA := md.DAG.Transactions[order[i]]

		for j := i + 1; j < len(order)-1; j++ {
			txB := md.DAG.Transactions[order[j]]

			// Victim must be from a different sender than attacker
			if txA.From == txB.From {
				continue
			}

			// Check for shared written state key (e.g. AMM pool reserve)
			sharedKey := findSharedKey(txA.WriteSet, txB.WriteSet)
			if sharedKey == "" {
				continue
			}

			for k := j + 1; k < len(order); k++ {
				txC := md.DAG.Transactions[order[k]]

				// TxC must belong to the same attacker/entity as TxA or have matching swap target
				if txC.From == txA.From {
					if _, touches := txC.WriteSet[sharedKey]; touches {
						// Gas price verification: TxA typically pays higher priority than TxB
						tipA := txA.EffectiveGasTip()
						tipB := txB.EffectiveGasTip()

						alerts = append(alerts, MEVAlert{
							Type:          MEVSandwich,
							Severity:      "HIGH",
							TxHashes:      []string{txA.Hash, txB.Hash, txC.Hash},
							ConflictedKey: sharedKey,
							Description: fmt.Sprintf("Sandwich attack detected on pool %s: Attacker (%s) front-ran victim (%s) with tip %s vs %s, then back-ran with %s",
								sharedKey, shortAddr(txA.From), shortAddr(txB.From), tipA.String(), tipB.String(), shortHash(txC.Hash)),
						})
					}
				}
			}
		}
	}

	return alerts
}

func (md *MEVDetector) detectFrontrunning() []MEVAlert {
	var alerts []MEVAlert
	order := md.DAG.Order

	for i := 0; i < len(order); i++ {
		txA := md.DAG.Transactions[order[i]]

		for j := i + 1; j < len(order); j++ {
			txB := md.DAG.Transactions[order[j]]

			if txA.From == txB.From {
				continue
			}

			sharedKey := findSharedKey(txA.WriteSet, txB.WriteSet)
			if sharedKey == "" {
				continue
			}

			tipA := txA.EffectiveGasTip()
			tipB := txB.EffectiveGasTip()

			// If TxA paid significantly higher tip (> 20% higher) to get placed before TxB on the same slot
			if tipA.Sign() > 0 && tipB.Sign() > 0 {
				diff := new(big.Int).Sub(tipA, tipB)
				if diff.Sign() > 0 {
					ratio := new(big.Int).Mul(diff, big.NewInt(100))
					ratio.Div(ratio, tipB)
					if ratio.Int64() >= 20 {
						alerts = append(alerts, MEVAlert{
							Type:          MEVFrontrun,
							Severity:      "MEDIUM",
							TxHashes:      []string{txA.Hash, txB.Hash},
							ConflictedKey: sharedKey,
							Description: fmt.Sprintf("Priority gas frontrunning on %s: %s (tip: %s) outbid %s (tip: %s) by %d%%",
								sharedKey, shortHash(txA.Hash), tipA.String(), shortHash(txB.Hash), tipB.String(), ratio.Int64()),
						})
					}
				}
			}
		}
	}

	return alerts
}

func (md *MEVDetector) detectHotspots(threshold int) []MEVAlert {
	var alerts []MEVAlert
	keyTouchers := make(map[string][]string)

	for _, txHash := range md.DAG.Order {
		tx := md.DAG.Transactions[txHash]
		for slot := range tx.WriteSet {
			keyTouchers[slot] = append(keyTouchers[slot], tx.Hash)
		}
	}

	for key, txs := range keyTouchers {
		if len(txs) >= threshold {
			alerts = append(alerts, MEVAlert{
				Type:          MEVContention,
				Severity:      "LOW",
				TxHashes:      txs,
				ConflictedKey: key,
				Description: fmt.Sprintf("State hotspot on slot %s: %d concurrent transactions contending for write access (serial execution bottleneck)",
					key, len(txs)),
			})
		}
	}

	return alerts
}

func findSharedKey(setA, setB map[string]struct{}) string {
	for k := range setA {
		if _, exists := setB[k]; exists {
			return k
		}
	}
	return ""
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
