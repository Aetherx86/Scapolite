package evm

import (
	"math/big"
	"testing"
)

func TestAccessModeString(t *testing.T) {
	cases := map[AccessMode]string{
		AccessRead:      "R",
		AccessWrite:     "W",
		AccessReadWrite: "RW",
		AccessMode(0):   "?",
	}
	for mode, want := range cases {
		if got := mode.String(); got != want {
			t.Errorf("AccessMode(%d).String() = %q, want %q", mode, got, want)
		}
	}
}

func TestERC20TransferParallelismSemantics(t *testing.T) {
	token := "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
	alice := "0x1111111111111111111111111111111111111111"
	bob := "0x2222222222222222222222222222222222222222"
	carol := "0x3333333333333333333333333333333333333333"
	dave := "0x4444444444444444444444444444444444444444"

	tx1 := NewTransaction("0x01", alice, token, 0, big.NewInt(20), big.NewInt(2), EncodeERC20Transfer(bob, big.NewInt(100)), nil)
	tx2 := NewTransaction("0x02", carol, token, 0, big.NewInt(20), big.NewInt(2), EncodeERC20Transfer(dave, big.NewInt(50)), nil)

	if tx1.Action != "erc20_transfer" {
		t.Fatalf("expected action erc20_transfer, got %q", tx1.Action)
	}
	if tx1.EffectiveGasLimit() != DefaultERC20Gas {
		t.Fatalf("expected default ERC20 gas %d, got %d", DefaultERC20Gas, tx1.EffectiveGasLimit())
	}

	// Neither zero-value ERC-20 transfer should mutate the token contract's native ETH balance.
	tokenBalKey := BalanceKey(token)
	if _, exists := tx1.WriteSet[tokenBalKey]; exists {
		t.Fatalf("zero-value ERC20 transfer unexpectedly wrote token native balance key %s", tokenBalKey)
	}

	// Disjoint ERC-20 transfers must have completely disjoint WriteSets and ReadSets.
	for k := range tx1.WriteSet {
		if _, overlap := tx2.WriteSet[k]; overlap {
			t.Fatalf("unexpected write overlap on %s between disjoint ERC20 transfers", k)
		}
		if _, overlap := tx2.ReadSet[k]; overlap {
			t.Fatalf("unexpected read/write overlap on %s between disjoint ERC20 transfers", k)
		}
	}

	// Overlapping recipient (bob) MUST share the ERC-20 balance slot.
	tx3 := NewTransaction("0x03", carol, token, 1, big.NewInt(20), big.NewInt(2), EncodeERC20Transfer(bob, big.NewInt(25)), nil)
	bobSlot := StorageKey{Address: token, Slot: "erc20:bal:" + bob}.String()
	if _, ok := tx1.WriteSet[bobSlot]; !ok {
		t.Fatalf("tx1 missing expected bob slot %s", bobSlot)
	}
	if _, ok := tx3.WriteSet[bobSlot]; !ok {
		t.Fatalf("tx3 missing expected bob slot %s", bobSlot)
	}
}

func TestERC20TransferFromAndApprove(t *testing.T) {
	token := "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
	owner := "0x1111111111111111111111111111111111111111"
	spender := "0x2222222222222222222222222222222222222222"
	dest := "0x3333333333333333333333333333333333333333"

	approveTx := NewTransaction("0xa1", owner, token, 0, big.NewInt(10), nil, EncodeERC20Approve(spender, big.NewInt(500)), nil)
	tfTx := NewTransaction("0xa2", spender, token, 0, big.NewInt(10), nil, EncodeERC20TransferFrom(owner, dest, big.NewInt(250)), nil)

	allowKey := StorageKey{Address: token, Slot: "erc20:allow:" + owner + ":" + spender}.String()
	if _, ok := approveTx.WriteSet[allowKey]; !ok {
		t.Fatalf("approve missing allowance write key %s", allowKey)
	}
	if _, ok := tfTx.ReadSet[allowKey]; !ok {
		t.Fatalf("transferFrom missing allowance read key %s", allowKey)
	}
	if _, ok := tfTx.WriteSet[allowKey]; !ok {
		t.Fatalf("transferFrom missing allowance write key %s", allowKey)
	}
}

func TestNativeETHBalanceAndContractCreate(t *testing.T) {
	alice := "0x1111111111111111111111111111111111111111"
	bob := "0x2222222222222222222222222222222222222222"

	ethTx := NewValueTransaction("0xe1", alice, bob, 0, big.NewInt(1_000_000), big.NewInt(20), big.NewInt(5), 21_000, nil, nil)
	if _, ok := ethTx.BalanceWrites[bob]; !ok {
		t.Fatalf("expected bob in BalanceWrites")
	}
	if _, ok := ethTx.BalanceReads[alice]; !ok {
		t.Fatalf("expected alice in BalanceReads")
	}
	if ethTx.EffectiveGasTip().Cmp(big.NewInt(5)) != 0 {
		t.Fatalf("expected tip 5, got %s", ethTx.EffectiveGasTip())
	}
	// Debit = value (1_000_000) + gasLimit (21_000) * gasPrice (20) = 1_420_000
	wantDebit := big.NewInt(1_420_000)
	if ethTx.EstimatedTotalDebit().Cmp(wantDebit) != 0 {
		t.Fatalf("EstimatedTotalDebit() = %s, want %s", ethTx.EstimatedTotalDebit(), wantDebit)
	}

	deployTx := NewValueTransaction("", alice, "", 1, nil, big.NewInt(20), nil, 0, []byte{0x60, 0x80, 0x60, 0x40}, nil)
	if deployTx.Action != "contract_create" {
		t.Fatalf("expected contract_create action, got %q", deployTx.Action)
	}
	if _, ok := deployTx.WriteSet[CreateKey(alice, 1)]; !ok {
		t.Fatalf("expected create key in WriteSet")
	}
	if deployTx.Hash == "" {
		t.Fatalf("expected deterministic hash to be generated")
	}
}

func TestOpaqueContractCallAndAccessList(t *testing.T) {
	contract := "0xcccccccccccccccccccccccccccccccccccccccc"
	slot := "0x000000000000000000000000000000000000000000000000000000000000000a"

	opaque := NewTransaction("0x1", "0xaaa1", contract, 0, big.NewInt(10), nil, []byte{0xde, 0xad, 0xbe, 0xef}, nil)
	opaqueKey := StorageKey{Address: contract, Slot: "contract:opaque_state"}.String()
	if _, ok := opaque.WriteSet[opaqueKey]; !ok {
		t.Fatalf("expected opaque fallback write key %s", opaqueKey)
	}

	withAL := NewTransaction("0x2", "0xaaa2", contract, 0, big.NewInt(10), nil, []byte{0xde, 0xad, 0xbe, 0xef}, []AccessTuple{
		{Address: contract, StorageKeys: []string{slot}},
	})
	if _, ok := withAL.WriteSet[opaqueKey]; ok {
		t.Fatalf("expected explicit access list to suppress opaque fallback key")
	}
	alKey := StorageKey{Address: contract, Slot: slot}.String()
	if _, ok := withAL.WriteSet[alKey]; !ok {
		t.Fatalf("expected access list key %s in WriteSet", alKey)
	}
}

func TestDeFiSelectorsWETHAndExplicitFootprints(t *testing.T) {
	router := "0x7a250d5630b4cf539739df2c5dacb4c659f2488d"
	weth := "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
	user := "0x1111111111111111111111111111111111111111"

	// Uniswap ETH swap touches both pool reserve slot and native balances.
	ethSwapData := []byte{0x7f, 0xf3, 0x6a, 0xb5, 0x00, 0x01}
	ethSwap := NewValueTransaction("0xs1", user, router, 0, big.NewInt(100), big.NewInt(20), nil, 0, ethSwapData, nil)
	if ethSwap.Action != "amm_swap" || ethSwap.EffectiveGasLimit() != DefaultAMMSwapGas {
		t.Fatalf("unexpected ETH swap decode: action=%s gas=%d", ethSwap.Action, ethSwap.EffectiveGasLimit())
	}
	if _, ok := ethSwap.WriteSet[StorageKey{Address: router, Slot: "amm:reserve_or_tick"}.String()]; !ok {
		t.Fatalf("missing AMM reserve slot")
	}
	if _, ok := ethSwap.BalanceWrites[router]; !ok {
		t.Fatalf("expected router native balance write on ETH swap")
	}

	// Uniswap addLiquidity touches both reserve slot and user LP slot.
	liq := NewTransaction("0xl1", user, router, 1, big.NewInt(20), nil, []byte{0xe8, 0xe3, 0x37, 0x00}, nil)
	if liq.Action != "amm_liquidity" {
		t.Fatalf("expected amm_liquidity, got %s", liq.Action)
	}
	if _, ok := liq.WriteSet[StorageKey{Address: router, Slot: "amm:lp:" + user}.String()]; !ok {
		t.Fatalf("missing LP write slot")
	}

	// WETH deposit & withdraw
	wrap := NewTransaction("0xw1", user, weth, 2, big.NewInt(20), nil, []byte{0xd0, 0xe3, 0x0d, 0xb0}, nil)
	unwrap := NewTransaction("0xw2", user, weth, 3, big.NewInt(20), nil, []byte{0x2e, 0x1a, 0x7d, 0x4d}, nil)
	if wrap.Action != "weth_wrap" || unwrap.Action != "weth_unwrap" {
		t.Fatalf("unexpected WETH actions: %s, %s", wrap.Action, unwrap.Action)
	}
	if _, ok := unwrap.BalanceWrites[weth]; !ok {
		t.Fatalf("expected WETH contract balance write on withdraw")
	}

	// Explicit ReadSlots, WriteSlots, and negative/positive BalanceDelta.
	custom := &Transaction{
		From:         user,
		To:           router,
		Nonce:        4,
		ReadSlots:    map[string][]string{router: {"slot_r"}},
		WriteSlots:   map[string][]string{router: {"slot_w"}},
		BalanceDelta: map[string]*big.Int{user: big.NewInt(-50), weth: big.NewInt(50)},
	}
	custom.DeriveStateAccess()
	if _, ok := custom.ReadSet[StorageKey{Address: router, Slot: "slot_r"}.String()]; !ok {
		t.Fatalf("missing explicit read slot")
	}
	if _, ok := custom.WriteSet[StorageKey{Address: router, Slot: "slot_w"}.String()]; !ok {
		t.Fatalf("missing explicit write slot")
	}
	if _, ok := custom.BalanceReads[user]; !ok {
		t.Fatalf("negative BalanceDelta should mark balance read")
	}
	if _, ok := custom.BalanceWrites[weth]; !ok {
		t.Fatalf("positive BalanceDelta should mark balance write")
	}
}
