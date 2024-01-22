// Copyright (C) 2022-2023, Chain4Travel AG. All rights reserved.
//
// This file is a derived work, based on ava-labs code whose
// original notices appear below.
//
// It is distributed under the same license conditions as the
// original code from which it is derived.
//
// Much love to the original authors for their work.
// **********************************************************
// (c) 2021, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package db

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/chain4travel/magellan/models"
	"github.com/chain4travel/magellan/services/indexes/params"
	"github.com/gocraft/dbr/v2"
	"github.com/stretchr/testify/require"
)

const (
	TestDB  = "mysql"
	TestDSN = "root:password@tcp(127.0.0.1:3306)/magellan_test?parseTime=true"
)

func TestTransaction(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()
	tm := time.Now().UTC().Truncate(1 * time.Second)

	v := &Transactions{}
	v.ID = "id"
	v.ChainID = "cid1"
	v.Type = "txtype"
	v.Memo = []byte("memo")
	v.CanonicalSerialization = []byte("cs")
	v.Txfee = 1
	v.Genesis = true
	v.CreatedAt = tm
	v.NetworkID = 1
	v.Status = 1

	stream := &dbr.NullEventReceiver{}

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableTransactions).Exec()

	err = p.InsertTransactions(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryTransactions(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}

	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}

	v.ChainID = "cid2"
	v.Type = "txtype1"
	v.Memo = []byte("memo1")
	v.CanonicalSerialization = []byte("cs1")
	v.Txfee = 2
	v.Genesis = false
	v.NetworkID = 2
	err = p.InsertTransactions(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err = p.QueryTransactions(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}

	if fv.NetworkID != 2 {
		t.Fatal("compare fail")
	}
	if fv.Txfee != 2 {
		t.Fatal("compare fail")
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestOutputsRedeeming(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()
	tm := time.Now().UTC().Truncate(1 * time.Second)

	v := &OutputsRedeeming{}
	v.ID = "id1"
	v.RedeemedAt = tm
	v.RedeemingTransactionID = "rtxid"
	v.Amount = 100
	v.OutputIndex = 1
	v.Intx = "intx1"
	v.AssetID = "aid1"
	v.ChainID = "cid1"
	v.CreatedAt = tm

	stream := &dbr.NullEventReceiver{}

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableOutputsRedeeming).Exec()

	err = p.InsertOutputsRedeeming(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryOutputsRedeeming(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}

	v.RedeemingTransactionID = "rtxid1"
	v.Amount = 102
	v.OutputIndex = 3
	v.Intx = "intx2"
	v.AssetID = "aid2"
	v.ChainID = "cid2"

	err = p.InsertOutputsRedeeming(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err = p.QueryOutputsRedeeming(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if fv.Intx != "intx2" {
		t.Fatal("compare fail")
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestOutputs(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()
	tm := time.Now().UTC().Truncate(1 * time.Second)

	v := &Outputs{}
	v.ID = "id1"
	v.ChainID = "cid1"
	v.TransactionID = "txid1"
	v.OutputIndex = 1
	v.AssetID = "aid1"
	v.OutputType = models.OutputTypesSECP2556K1Transfer
	v.Amount = 2
	v.Locktime = 3
	v.Threshold = 4
	v.GroupID = 5
	v.Payload = []byte("payload")
	v.StakeLocktime = 6
	v.Stake = true
	v.Frozen = true
	v.Stakeableout = true
	v.Genesisutxo = true
	v.CreatedAt = tm

	stream := &dbr.NullEventReceiver{}

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableOutputs).Exec()

	err = p.InsertOutputs(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryOutputs(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}

	v.ChainID = "cid2"
	v.TransactionID = "txid2"
	v.OutputIndex = 2
	v.AssetID = "aid2"
	v.OutputType = models.OutputTypesSECP2556K1Mint
	v.Amount = 3
	v.Locktime = 4
	v.Threshold = 5
	v.GroupID = 6
	v.Payload = []byte("payload2")
	v.StakeLocktime = 7
	v.Stake = false
	v.Frozen = false
	v.Stakeableout = false
	v.Genesisutxo = false

	err = p.InsertOutputs(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err = p.QueryOutputs(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if fv.Amount != 3 {
		t.Fatal("compare fail")
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestAssets(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()
	tm := time.Now().UTC().Truncate(1 * time.Second)

	v := &Assets{}
	v.ID = "id1"
	v.ChainID = "cid1"
	v.Name = "name1"
	v.Symbol = "symbol1"
	v.Denomination = 0x1
	v.Alias = "alias1"
	v.CurrentSupply = 1
	v.CreatedAt = tm

	stream := &dbr.NullEventReceiver{}

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableAssets).Exec()

	err = p.InsertAssets(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryAssets(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}

	v.ChainID = "cid2"
	v.Name = "name2"
	v.Symbol = "symbol2"
	v.Denomination = 0x2
	v.Alias = "alias2"
	v.CurrentSupply = 2
	v.CreatedAt = tm

	err = p.InsertAssets(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err = p.QueryAssets(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if fv.Name != "name2" {
		t.Fatal("compare fail")
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestAddresses(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()
	tm := time.Now().UTC().Truncate(1 * time.Second)
	tmu := time.Now().UTC().Truncate(1 * time.Second)

	basebin := [33]byte{}
	for cnt := 0; cnt < len(basebin); cnt++ {
		basebin[cnt] = byte(cnt + 1)
	}

	v := &Addresses{}
	v.Address = "id1"
	v.PublicKey = make([]byte, len(basebin))
	copy(v.PublicKey, basebin[:])
	v.CreatedAt = tm
	v.UpdatedAt = tmu

	stream := &dbr.NullEventReceiver{}

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableAddresses).Exec()

	err = p.InsertAddresses(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryAddresses(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}

	basebin[0] = 0xF
	basebin[5] = 0xE
	copy(v.PublicKey, basebin[:])
	v.CreatedAt = tm
	v.UpdatedAt = tmu.Add(1 * time.Minute)

	err = p.InsertAddresses(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err = p.QueryAddresses(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if fv.PublicKey[0] != 0xF {
		t.Fatal("compare fail")
	}
	if fv.PublicKey[5] != 0xE {
		t.Fatal("compare fail")
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestAddressChain(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()
	tm := time.Now().UTC().Truncate(1 * time.Second)
	tmu := time.Now().UTC().Truncate(1 * time.Second)

	v := &AddressChain{}
	v.Address = "id1"
	v.ChainID = "ch1"
	v.CreatedAt = tm
	v.UpdatedAt = tmu

	stream := &dbr.NullEventReceiver{}

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableAddressChain).Exec()

	err = p.InsertAddressChain(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryAddressChain(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}

	v.ChainID = "ch2"
	v.CreatedAt = tm
	v.UpdatedAt = tmu.Add(1 * time.Minute)

	err = p.InsertAddressChain(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err = p.QueryAddressChain(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if fv.ChainID != "ch2" {
		t.Fatal("compare fail")
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestOutputAddresses(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()
	tm := time.Now().UTC().Truncate(1 * time.Second)
	tmu := time.Now().UTC().Truncate(1 * time.Second)

	stream := &dbr.NullEventReceiver{}

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableOutputAddresses).Exec()

	v := &OutputAddresses{}
	v.OutputID = "oid1"
	v.Address = "id1"
	v.CreatedAt = tm
	v.UpdatedAt = tmu

	err = p.InsertOutputAddresses(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryOutputAddresses(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if fv.RedeemingSignature != nil {
		t.Fatal("compare fail")
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}

	v.OutputID = "oid1"
	v.Address = "id1"
	v.RedeemingSignature = []byte("rd1")
	v.CreatedAt = tm
	v.UpdatedAt = tmu.Add(1 * time.Minute)

	err = p.InsertOutputAddresses(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err = p.QueryOutputAddresses(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if fv.UpdatedAt != tmu.Add(1*time.Minute) {
		t.Fatal("compare fail")
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}

	v.RedeemingSignature = []byte("rd2")
	v.CreatedAt = tm
	v.UpdatedAt = tmu.Add(2 * time.Minute)

	err = p.InsertOutputAddresses(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err = p.QueryOutputAddresses(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if string(v.RedeemingSignature) != "rd2" {
		t.Fatal("compare fail")
	}
	if fv.UpdatedAt != tmu.Add(2*time.Minute) {
		t.Fatal("compare fail")
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}

	v.RedeemingSignature = []byte("rd3")
	v.CreatedAt = tm
	v.UpdatedAt = tmu.Add(3 * time.Minute)

	err = p.UpdateOutputAddresses(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("update fail", err)
	}
	fv, err = p.QueryOutputAddresses(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if string(v.RedeemingSignature) != "rd3" {
		t.Fatal("compare fail")
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestTransactionsEpoch(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()
	tm := time.Now().UTC().Truncate(1 * time.Second)

	v := &TransactionsEpoch{}
	v.ID = "id1"
	v.Epoch = 10
	v.VertexID = "vid1"
	v.CreatedAt = tm

	stream := &dbr.NullEventReceiver{}

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableTransactionsEpochs).Exec()

	err = p.InsertTransactionsEpoch(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryTransactionsEpoch(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}

	v.Epoch = 11
	v.VertexID = "vid2"
	v.CreatedAt = tm

	err = p.InsertTransactionsEpoch(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err = p.QueryTransactionsEpoch(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if fv.VertexID != "vid2" {
		t.Fatal("compare fail")
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestCvmBlocks(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()
	tm := time.Now().UTC().Truncate(1 * time.Second)

	v := &CvmBlocks{}
	v.Block = "1"
	v.Serialization = []byte("{}")
	v.Hash = "0x"
	v.CreatedAt = tm

	stream := &dbr.NullEventReceiver{}

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableCvmBlocks).Exec()

	err = p.InsertCvmBlocks(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryCvmBlock(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestCvmAddresses(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()
	tm := time.Now().UTC().Truncate(1 * time.Second)

	v := &CvmAddresses{}
	v.ID = "id1"
	v.Type = models.CChainIn
	v.Idx = 1
	v.TransactionID = "tid1"
	v.Address = "addr1"
	v.AssetID = "assid1"
	v.Amount = 2
	v.Nonce = 3
	v.CreatedAt = tm

	stream := &dbr.NullEventReceiver{}

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableCvmAddresses).Exec()

	err = p.InsertCvmAddresses(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryCvmAddresses(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}

	v.Type = models.CchainOut
	v.Idx = 2
	v.TransactionID = "tid2"
	v.Address = "addr2"
	v.AssetID = "assid2"
	v.Amount = 3
	v.Nonce = 4
	v.CreatedAt = tm

	err = p.InsertCvmAddresses(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err = p.QueryCvmAddresses(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if fv.Idx != 2 {
		t.Fatal("compare fail")
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestCvmTransactions(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()
	tm := time.Now().UTC().Truncate(1 * time.Second).Add(-1 * time.Hour)

	v := &CvmTransactionsAtomic{}
	v.TransactionID = "trid1"
	v.Type = models.CChainIn
	v.ChainID = "bid1"
	v.Block = "1"
	v.CreatedAt = tm

	stream := &dbr.NullEventReceiver{}

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableCvmTransactionsAtomic).Exec()

	err = p.InsertCvmTransactionsAtomic(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryCvmTransactionsAtomic(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}

	tm = time.Now().UTC().Truncate(1 * time.Second).Add(-1 * time.Hour)

	v.Type = models.CchainOut
	v.TransactionID = "trid2"
	v.ChainID = "bid2"
	v.Block = "2"
	v.CreatedAt = tm

	err = p.InsertCvmTransactionsAtomic(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err = p.QueryCvmTransactionsAtomic(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !fv.CreatedAt.Equal(tm) {
		t.Fatal("compare fail")
	}
	if fv.TransactionID != "trid2" {
		t.Fatal("compare fail")
	}
	if fv.Block != "2" {
		t.Fatal("compare fail")
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestCvmTransactionsTxdata(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()
	tm := time.Now().UTC().Truncate(1 * time.Second)

	v := &CvmTransactionsTxdata{}
	v.Hash = "h1"
	v.Block = "1"
	v.Idx = 1
	v.CreatedAt = tm
	v.Serialization = []byte("test123")

	stream := &dbr.NullEventReceiver{}

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableCvmTransactionsTxdata).Exec()

	err = p.InsertCvmAccount(ctx, rawDBConn.NewSession(stream), &CvmAccount{}, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	err = p.InsertCvmTransactionsTxdata(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryCvmTransactionsTxdata(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}

	v.Idx = 7
	v.CreatedAt = tm
	v.Serialization = []byte("test456")

	err = p.InsertCvmTransactionsTxdata(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err = p.QueryCvmTransactionsTxdata(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if string(fv.Serialization) != "test456" {
		t.Fatal("compare fail")
	}
	if fv.Hash != "h1" {
		t.Fatal("compare fail")
	}
	if fv.Idx != 7 {
		t.Fatal("compare fail")
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestPvmBlocks(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()
	tm := time.Now().UTC().Truncate(1 * time.Second)

	v := &PvmBlocks{}
	v.ID = "id1"
	v.ChainID = "cid1"
	v.Type = models.BlockTypeAbort
	v.ParentID = "pid1"
	v.Serialization = []byte("ser1")
	v.CreatedAt = tm

	stream := &dbr.NullEventReceiver{}

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TablePvmBlocks).Exec()

	err = p.InsertPvmBlocks(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryPvmBlocks(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}

	v.ChainID = "cid2"
	v.Type = models.BlockTypeCommit
	v.ParentID = "pid2"
	v.Serialization = []byte("ser2")
	v.CreatedAt = tm

	err = p.InsertPvmBlocks(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err = p.QueryPvmBlocks(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if string(fv.Serialization) != "ser2" {
		t.Fatal("compare fail")
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestTransactionsValidator(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()
	tm := time.Now().UTC().Truncate(1 * time.Second)

	v := &TransactionsValidator{}
	v.ID = "id1"
	v.NodeID = "nid1"
	v.Start = 1
	v.End = 2
	v.CreatedAt = tm

	stream := &dbr.NullEventReceiver{}

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableTransactionsValidator).Exec()

	err = p.InsertTransactionsValidator(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryTransactionsValidator(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}

	v.NodeID = "nid2"
	v.Start = 2
	v.End = 3
	v.CreatedAt = tm

	err = p.InsertTransactionsValidator(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err = p.QueryTransactionsValidator(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if v.NodeID != "nid2" {
		t.Fatal("compare fail")
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestTransactionsBlock(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()
	tm := time.Now().UTC().Truncate(1 * time.Second)

	v := &TransactionsBlock{}
	v.ID = "id1"
	v.TxBlockID = "txb1"
	v.CreatedAt = tm

	stream := &dbr.NullEventReceiver{}

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableTransactionsBlock).Exec()

	err = p.InsertTransactionsBlock(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryTransactionsBlock(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}

	v.TxBlockID = "txb2"
	v.CreatedAt = tm

	err = p.InsertTransactionsBlock(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err = p.QueryTransactionsBlock(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if v.TxBlockID != "txb2" {
		t.Fatal("compare fail")
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestAddressBech32(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()
	tmu := time.Now().UTC().Truncate(1 * time.Second)

	v := &AddressBech32{}
	v.Address = "adr1"
	v.Bech32Address = "badr1"
	v.UpdatedAt = tmu

	stream := &dbr.NullEventReceiver{}

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableAddressBech32).Exec()

	err = p.InsertAddressBech32(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryAddressBech32(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}

	v.Bech32Address = "badr2"
	v.UpdatedAt = tmu.Add(1 * time.Minute)

	err = p.InsertAddressBech32(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err = p.QueryAddressBech32(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if v.Bech32Address != "badr2" {
		t.Fatal("compare fail")
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestOutputAddressAccumulateOut(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()

	v := &OutputAddressAccumulate{}
	v.OutputID = "out1"
	v.Address = "adr1"
	v.TransactionID = "txid1"
	v.OutputIndex = 1
	v.CreatedAt = time.Now().UTC().Truncate(1 * time.Second)

	v.ComputeID()

	stream := &dbr.NullEventReceiver{}

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableOutputAddressAccumulateOut).Exec()

	err = p.InsertOutputAddressAccumulateOut(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryOutputAddressAccumulateOut(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}

	v.OutputIndex = 3
	v.TransactionID = "tr3"

	err = p.InsertOutputAddressAccumulateOut(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err = p.QueryOutputAddressAccumulateOut(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if v.OutputIndex != 3 {
		t.Fatal("compare fail")
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestOutputAddressAccumulateIn(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()

	v := &OutputAddressAccumulate{}
	v.OutputID = "out1"
	v.Address = "adr1"
	v.TransactionID = "txid1"
	v.OutputIndex = 1
	v.CreatedAt = time.Now().UTC().Truncate(1 * time.Second)

	v.ComputeID()

	stream := &dbr.NullEventReceiver{}

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableOutputAddressAccumulateIn).Exec()

	err = p.InsertOutputAddressAccumulateIn(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryOutputAddressAccumulateIn(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}

	v.OutputIndex = 3
	v.TransactionID = "tr3"

	err = p.InsertOutputAddressAccumulateIn(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err = p.QueryOutputAddressAccumulateIn(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if v.OutputIndex != 3 {
		t.Fatal("compare fail")
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestOutputTxsAccumulate(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()

	v := &OutputTxsAccumulate{}
	v.ChainID = "ch1"
	v.AssetID = "asset1"
	v.Address = "adr1"
	v.TransactionID = "tr1"
	v.CreatedAt = time.Now().UTC().Truncate(1 * time.Second)

	v.ComputeID()

	stream := &dbr.NullEventReceiver{}

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableOutputTxsAccumulate).Exec()

	err = p.InsertOutputTxsAccumulate(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryOutputTxsAccumulate(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestAccumulateBalancesReceived(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()

	v := &AccumulateBalancesAmount{}
	v.ChainID = "ch1"
	v.AssetID = "asset1"
	v.Address = "adr1"
	v.TotalAmount = "0"
	v.UtxoCount = "0"
	v.UpdatedAt = time.Now().UTC().Truncate(1 * time.Second)

	v.ComputeID()

	stream := &dbr.NullEventReceiver{}

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableAccumulateBalancesReceived).Exec()

	err = p.InsertAccumulateBalancesReceived(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryAccumulateBalancesReceived(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestAccumulateBalancesSent(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()

	v := &AccumulateBalancesAmount{}
	v.ChainID = "ch1"
	v.AssetID = "asset1"
	v.Address = "adr1"
	v.TotalAmount = "0"
	v.UtxoCount = "0"
	v.UpdatedAt = time.Now().UTC().Truncate(1 * time.Second)

	v.ComputeID()

	stream := &dbr.NullEventReceiver{}

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableAccumulateBalancesSent).Exec()

	err = p.InsertAccumulateBalancesSent(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryAccumulateBalancesSent(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestAccumulateBalancesTransactions(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()

	v := &AccumulateBalancesTransactions{}
	v.ChainID = "ch1"
	v.AssetID = "asset1"
	v.Address = "adr1"
	v.TransactionCount = "0"
	v.UpdatedAt = time.Now().UTC().Truncate(1 * time.Second)

	v.ComputeID()

	stream := &dbr.NullEventReceiver{}

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableAccumulateBalancesTransactions).Exec()

	err = p.InsertAccumulateBalancesTransactions(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryAccumulateBalancesTransactions(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestTxPool(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()

	v := &TxPool{}
	v.NetworkID = 1
	v.ChainID = "ch1"
	v.Serialization = []byte("hello")
	v.Topic = "topic1"
	v.MsgKey = "key1"
	v.CreatedAt = time.Now().UTC().Truncate(1 * time.Second)

	v.ComputeID()

	stream := &dbr.NullEventReceiver{}

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableTxPool).Exec()

	err = p.InsertTxPool(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryTxPool(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestKeyValueStore(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()

	v := &KeyValueStore{}
	v.K = "k"
	v.V = "v"

	stream := &dbr.NullEventReceiver{}

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableKeyValueStore).Exec()

	err = p.InsertKeyValueStore(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryKeyValueStore(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestNodeIndex(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()

	v := &NodeIndex{}
	v.Instance = "def"
	v.Topic = "top"
	v.Idx = 1

	stream := &dbr.NullEventReceiver{}

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableNodeIndex).Exec()

	err = p.InsertNodeIndex(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err := p.QueryNodeIndex(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}

	v.Idx = 2

	err = p.InsertNodeIndex(ctx, rawDBConn.NewSession(stream), v, true)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err = p.QueryNodeIndex(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if fv.Idx != 2 {
		t.Fatal("compare fail")
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}

	v.Idx = 3
	err = p.UpdateNodeIndex(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	fv, err = p.QueryNodeIndex(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if fv.Idx != 3 {
		t.Fatal("compare fail")
	}
	if !reflect.DeepEqual(*v, *fv) {
		t.Fatal("compare fail")
	}
}

func TestInsertMultisigAlias(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()
	stream := &dbr.NullEventReceiver{}

	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	if err != nil {
		t.Fatal("db fail", err)
	}
	_, _ = rawDBConn.NewSession(stream).DeleteFrom(TableMultisigAliases).Exec()

	v := &MultisigAlias{}
	v.Alias = "abcdefghijklmnopqrstABCDEF1234567"
	v.Owner = "ABCDEFghijklmnopqrstabcdef1234567"
	v.Memo = "Memo"
	v.Bech32Address = "kopernikus1vscyf7czawylztn6ghhg0z27swwewxgzgpcxvy"
	v.TransactionID = "abcdefghijklmnopqrstABCDEF1234567abcdefghijklmnop"
	v.CreatedAt = time.Now().UTC().Truncate(1 * time.Second)

	err = p.InsertMultisigAlias(ctx, rawDBConn.NewSession(stream), v)
	if err != nil {
		t.Fatal("insert fail", err)
	}
	err = p.InsertAddressBech32(ctx, rawDBConn.NewSession(stream), &AddressBech32{Address: v.Alias, Bech32Address: "kopernikus1vscyf7czawylztn6ghhg0z27swwewxgzgpcxvy", UpdatedAt: time.Now().UTC().Truncate(1 * time.Second)}, false)
	if err != nil {
		t.Fatal("insert address bech32 fail", err)
	}

	owners := []string{v.Owner}
	fv, err := p.QueryMultisigAliasesForOwners(ctx, rawDBConn.NewSession(stream), owners)
	if err != nil {
		t.Fatal("query fail", err)
	}
	if !reflect.DeepEqual(v.Bech32Address, (*fv)[0].Bech32Address) {
		t.Fatal("compare fail")
	}
	err = p.DeleteMultisigAlias(ctx, rawDBConn.NewSession(stream), v.Alias)
	if err != nil {
		t.Fatal("delete fail", err)
	}
}

func TestInsertDACProposal(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()
	stream := &dbr.NullEventReceiver{}
	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	require.NoError(t, err)
	_, err = rawDBConn.NewSession(stream).DeleteFrom(TableTransactions).Exec()
	require.NoError(t, err)
	_, err = rawDBConn.NewSession(stream).DeleteFrom(TableDACVotes).Exec()
	require.NoError(t, err)
	_, err = rawDBConn.NewSession(stream).DeleteFrom(TableDACProposals).Exec()
	require.NoError(t, err)
	now := time.Now().UTC().Truncate(time.Second)

	baseTxMemo := []byte("serialized base tx memo")
	proposal := DACProposal{
		ID:              "1111111111111111111111111111111111111111111111111",
		ProposerAddr:    "222222222222222222222222222222222",
		StartTime:       now.Add(1 * time.Second),
		EndTime:         now.Add(10 * time.Second),
		Type:            models.ProposalTypeBaseFee,
		IsAdminProposal: false,
		SerializedBytes: []byte("serialized proposal bytes"),
		Options:         []byte("serialized proposal options"),
		Data:            []byte("serialized proposal data"),
		Memo:            []byte("should be ignored"),           // should be ingored and not inserted
		Outcome:         []byte("serialized proposal outcome"), // should be ingored and not inserted
		Status:          models.ProposalStatusInProgress,
	}

	require.NoError(t, p.InsertTransactions(ctx, rawDBConn.NewSession(stream), &Transactions{
		ID:        proposal.ID,
		Memo:      baseTxMemo,
		CreatedAt: now,
	}, false))
	require.NoError(t, p.InsertDACProposal(ctx, rawDBConn.NewSession(stream), &proposal))

	expectedProposal := proposal
	expectedProposal.Memo = baseTxMemo
	expectedProposal.Outcome = nil
	resultProposals, err := p.QueryDACProposals(ctx, rawDBConn.NewSession(stream), &params.ListDACProposalsParams{})
	require.NoError(t, err)
	require.Equal(t, []DACProposal{expectedProposal}, resultProposals)
}

func TestUpdateDACProposal(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()
	stream := &dbr.NullEventReceiver{}
	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	require.NoError(t, err)
	_, err = rawDBConn.NewSession(stream).DeleteFrom(TableTransactions).Exec()
	require.NoError(t, err)
	_, err = rawDBConn.NewSession(stream).DeleteFrom(TableDACVotes).Exec()
	require.NoError(t, err)
	_, err = rawDBConn.NewSession(stream).DeleteFrom(TableDACProposals).Exec()
	require.NoError(t, err)
	now := time.Now().UTC().Truncate(time.Second)

	proposal := DACProposal{
		ID:              "1111111111111111111111111111111111111111111111111",
		ProposerAddr:    "222222222222222222222222222222222",
		StartTime:       now.Add(1 * time.Second),
		EndTime:         now.Add(10 * time.Second),
		Type:            models.ProposalTypeBaseFee,
		IsAdminProposal: false,
		SerializedBytes: []byte("serialized proposal bytes"),
		Options:         []byte("serialized proposal options"),
		Data:            []byte("serialized proposal data"),
		Memo:            []byte("serialized base tx memo"),
		Status:          models.ProposalStatusInProgress,
	}
	require.NoError(t, p.InsertTransactions(ctx, rawDBConn.NewSession(stream), &Transactions{
		ID:        proposal.ID,
		Memo:      proposal.Memo,
		CreatedAt: now,
	}, false))
	require.NoError(t, p.InsertDACProposal(ctx, rawDBConn.NewSession(stream), &proposal))

	updatedProposalBytes := []byte("updated serialized proposal bytes")
	require.NoError(t, p.UpdateDACProposal(
		ctx,
		rawDBConn.NewSession(stream),
		proposal.ID,
		updatedProposalBytes,
	))

	expectedProposal := proposal
	expectedProposal.SerializedBytes = updatedProposalBytes
	resultProposals, err := p.QueryDACProposals(ctx, rawDBConn.NewSession(stream), &params.ListDACProposalsParams{})
	require.NoError(t, err)
	require.Equal(t, []DACProposal{expectedProposal}, resultProposals)
}

func TestFinishDACProposals(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()
	stream := &dbr.NullEventReceiver{}
	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	require.NoError(t, err)
	_, err = rawDBConn.NewSession(stream).DeleteFrom(TableTransactions).Exec()
	require.NoError(t, err)
	_, err = rawDBConn.NewSession(stream).DeleteFrom(TableDACVotes).Exec()
	require.NoError(t, err)
	_, err = rawDBConn.NewSession(stream).DeleteFrom(TableDACProposals).Exec()
	require.NoError(t, err)
	now := time.Now().UTC().Truncate(time.Second)

	proposals := []*DACProposal{
		{ // 0
			ID:              "1111111111111111111111111111111111111111111111111",
			ProposerAddr:    "111111111111111111111111111111111",
			StartTime:       now.Add(1 * time.Second),
			EndTime:         now.Add(10 * time.Second),
			Type:            models.ProposalTypeBaseFee,
			IsAdminProposal: false,
			SerializedBytes: []byte("1 serialized proposal bytes 1"),
			Options:         []byte("1 serialized proposal options 1"),
			Data:            []byte("1 serialized proposal data 1"),
			Memo:            []byte("1 serialized proposal memo 1"),
			Status:          models.ProposalStatusInProgress,
		},
		{ // 1
			ID:              "2222222222222222222222222222222222222222222222222",
			ProposerAddr:    "222222222222222222222222222222222",
			StartTime:       now.Add(1 * time.Second),
			EndTime:         now.Add(10 * time.Second),
			Type:            models.ProposalTypeBaseFee,
			IsAdminProposal: false,
			SerializedBytes: []byte("2 serialized proposal bytes 2"),
			Options:         []byte("2 serialized proposal options 2"),
			Data:            []byte("2 serialized proposal data 2"),
			Memo:            []byte("2 serialized proposal memo 2"),
			Status:          models.ProposalStatusInProgress,
		},
	}

	for _, proposal := range proposals {
		require.NoError(t, p.InsertTransactions(ctx, rawDBConn.NewSession(stream), &Transactions{
			ID:        proposal.ID,
			Memo:      proposal.Memo,
			CreatedAt: now,
		}, false))
		require.NoError(t, p.InsertDACProposal(ctx, rawDBConn.NewSession(stream), proposal))
	}

	finishTime := now.Add(5 * time.Second)

	require.Error(t, p.FinishDACProposals(
		ctx,
		rawDBConn.NewSession(stream),
		[]string{"3333333333333333333333333333333333333333333333333"}, // non-existing
		finishTime,
		models.ProposalStatusFailed,
	))

	require.NoError(t, p.FinishDACProposals(
		ctx,
		rawDBConn.NewSession(stream),
		[]string{proposals[0].ID, proposals[1].ID},
		finishTime,
		models.ProposalStatusFailed,
	))

	expectedProposal0 := *proposals[0]
	expectedProposal0.Status = models.ProposalStatusFailed
	expectedProposal0.FinishedAt = &finishTime
	expectedProposal1 := *proposals[1]
	expectedProposal1.Status = models.ProposalStatusFailed
	expectedProposal1.FinishedAt = &finishTime
	resultProposals, err := p.QueryDACProposals(ctx, rawDBConn.NewSession(stream), &params.ListDACProposalsParams{})
	require.NoError(t, err)
	require.Equal(t, []DACProposal{expectedProposal0, expectedProposal1}, resultProposals)
}

func TestFinishDACProposalWithOutcome(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()
	stream := &dbr.NullEventReceiver{}
	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	require.NoError(t, err)
	_, err = rawDBConn.NewSession(stream).DeleteFrom(TableTransactions).Exec()
	require.NoError(t, err)
	_, err = rawDBConn.NewSession(stream).DeleteFrom(TableDACVotes).Exec()
	require.NoError(t, err)
	_, err = rawDBConn.NewSession(stream).DeleteFrom(TableDACProposals).Exec()
	require.NoError(t, err)
	now := time.Now().UTC().Truncate(time.Second)

	proposal := DACProposal{
		ID:              "1111111111111111111111111111111111111111111111111",
		ProposerAddr:    "222222222222222222222222222222222",
		StartTime:       now.Add(1 * time.Second),
		EndTime:         now.Add(10 * time.Second),
		Type:            models.ProposalTypeBaseFee,
		IsAdminProposal: false,
		SerializedBytes: []byte("serialized proposal bytes"),
		Options:         []byte("serialized proposal options"),
		Data:            []byte("serialized proposal data"),
		Memo:            []byte("serialized base tx memo"),
		Status:          models.ProposalStatusInProgress,
	}
	require.NoError(t, p.InsertTransactions(ctx, rawDBConn.NewSession(stream), &Transactions{
		ID:        proposal.ID,
		Memo:      proposal.Memo,
		CreatedAt: now,
	}, false))
	require.NoError(t, p.InsertDACProposal(ctx, rawDBConn.NewSession(stream), &proposal))

	finishTime := now.Add(5 * time.Second)
	outcome := []byte{}

	require.Error(t, p.FinishDACProposalWithOutcome(
		ctx,
		rawDBConn.NewSession(stream),
		"3333333333333333333333333333333333333333333333333",
		finishTime,
		models.ProposalStatusSuccess,
		outcome,
	))

	require.NoError(t, p.FinishDACProposalWithOutcome(
		ctx,
		rawDBConn.NewSession(stream),
		proposal.ID,
		finishTime,
		models.ProposalStatusSuccess,
		outcome,
	))

	expectedProposal := proposal
	expectedProposal.Status = models.ProposalStatusSuccess
	expectedProposal.Outcome = outcome
	expectedProposal.FinishedAt = &finishTime
	resultProposals, err := p.QueryDACProposals(ctx, rawDBConn.NewSession(stream), &params.ListDACProposalsParams{})
	require.NoError(t, err)
	require.Equal(t, []DACProposal{expectedProposal}, resultProposals)
}

func TestGetDACProposals(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()
	stream := &dbr.NullEventReceiver{}
	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	require.NoError(t, err)
	_, err = rawDBConn.NewSession(stream).DeleteFrom(TableTransactions).Exec()
	require.NoError(t, err)
	_, err = rawDBConn.NewSession(stream).DeleteFrom(TableDACVotes).Exec()
	require.NoError(t, err)
	_, err = rawDBConn.NewSession(stream).DeleteFrom(TableDACProposals).Exec()
	require.NoError(t, err)
	now := time.Now().UTC().Truncate(time.Second)

	proposals := []*DACProposal{
		{ // 0
			ID:              "1111111111111111111111111111111111111111111111111",
			ProposerAddr:    "111111111111111111111111111111111",
			StartTime:       now.Add(100 * time.Second),
			EndTime:         now.Add(100 * time.Second).Add(time.Hour),
			Type:            models.ProposalTypeBaseFee,
			IsAdminProposal: false,
			SerializedBytes: []byte("1 serialized proposal bytes 1"),
			Options:         []byte("1 serialized proposal options 1"),
			Data:            []byte("1 serialized proposal data 1"),
			Memo:            []byte("1 serialized proposal memo 1"),
			Status:          models.ProposalStatusInProgress,
		},
		{ // 1
			ID:              "2222222222222222222222222222222222222222222222222",
			ProposerAddr:    "222222222222222222222222222222222",
			StartTime:       now.Add(100 * time.Second),
			EndTime:         now.Add(100 * time.Second).Add(time.Hour),
			Type:            models.ProposalType(100), // different proposal type
			IsAdminProposal: false,
			SerializedBytes: []byte("2 serialized proposal bytes 2"),
			Options:         []byte("2 serialized proposal options 2"),
			Data:            []byte("2 serialized proposal data 2"),
			Memo:            []byte("2 serialized proposal memo 2"),
			Status:          models.ProposalStatusInProgress,
		},
		{ // 2
			ID:              "3333333333333333333333333333333333333333333333333",
			ProposerAddr:    "333333333333333333333333333333333",
			StartTime:       now.Add(100 * time.Second),
			EndTime:         now.Add(100 * time.Second).Add(time.Hour),
			Type:            models.ProposalTypeBaseFee,
			IsAdminProposal: false,
			SerializedBytes: []byte("3 serialized proposal bytes 3"),
			Options:         []byte("3 serialized proposal options 3"),
			Data:            []byte("3 serialized proposal data 3"),
			Memo:            []byte("3 serialized proposal memo 3"),
			Status:          models.ProposalStatusInProgress,
		},
		{ // 3
			ID:              "4444444444444444444444444444444444444444444444444",
			ProposerAddr:    "444444444444444444444444444444444",
			StartTime:       now.Add(100 * time.Second),
			EndTime:         now.Add(100 * time.Second).Add(time.Hour),
			Type:            models.ProposalTypeBaseFee,
			IsAdminProposal: false,
			SerializedBytes: []byte("4 serialized proposal bytes 4"),
			Options:         []byte("4 serialized proposal options 4"),
			Data:            []byte("4 serialized proposal data 4"),
			Memo:            []byte("4 serialized proposal memo 4"),
			Status:          models.ProposalStatusInProgress,
		},
	}

	for _, proposal := range proposals {
		require.NoError(t, p.InsertTransactions(ctx, rawDBConn.NewSession(stream), &Transactions{
			ID:        proposal.ID,
			Memo:      proposal.Memo,
			CreatedAt: now,
		}, false))
		require.NoError(t, p.InsertDACProposal(ctx, rawDBConn.NewSession(stream), proposal))
	}

	resultProposals, err := p.GetDACProposals(ctx, rawDBConn.NewSession(stream), []string{proposals[1].ID, proposals[2].ID})
	require.NoError(t, err)
	require.Equal(t, []DACProposal{*proposals[1], *proposals[2]}, resultProposals)
}

func TestQueryDACProposals(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()
	stream := &dbr.NullEventReceiver{}
	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	require.NoError(t, err)
	_, err = rawDBConn.NewSession(stream).DeleteFrom(TableTransactions).Exec()
	require.NoError(t, err)
	_, err = rawDBConn.NewSession(stream).DeleteFrom(TableDACVotes).Exec()
	require.NoError(t, err)
	_, err = rawDBConn.NewSession(stream).DeleteFrom(TableDACProposals).Exec()
	require.NoError(t, err)
	now := time.Now().UTC().Truncate(time.Second)

	baseFeeProposalType := models.ProposalTypeBaseFee
	proposalStatusSuccess := models.ProposalStatusSuccess
	queryParams := &params.ListDACProposalsParams{
		ListParams: params.ListParams{
			Limit:  3,
			Offset: 1,
		},
		MinStartTime:         now.Add(100 * time.Second),
		MaxStartTime:         now.Add(105 * time.Second),
		MinStartTimeProvided: true,
		MaxStartTimeProvided: true,
		ProposalType:         &baseFeeProposalType,
		ProposalStatus:       &proposalStatusSuccess,
	}

	proposals := []*DACProposal{
		{ // 0
			ID:              "1111111111111111111111111111111111111111111111111",
			ProposerAddr:    "111111111111111111111111111111111",
			StartTime:       queryParams.MinStartTime,
			EndTime:         queryParams.MinStartTime.Add(time.Hour),
			Type:            *queryParams.ProposalType,
			IsAdminProposal: false,
			SerializedBytes: []byte("1 serialized proposal bytes 1"),
			Options:         []byte("1 serialized proposal options 1"),
			Data:            []byte("1 serialized proposal data 1"),
			Memo:            []byte("1 serialized proposal memo 1"),
			Status:          models.ProposalStatusInProgress, // different proposal status
		},
		{ // 1
			ID:              "2222222222222222222222222222222222222222222222222",
			ProposerAddr:    "222222222222222222222222222222222",
			StartTime:       queryParams.MinStartTime,
			EndTime:         queryParams.MinStartTime.Add(time.Hour),
			Type:            models.ProposalType(100), // different proposal type
			IsAdminProposal: false,
			SerializedBytes: []byte("2 serialized proposal bytes 2"),
			Options:         []byte("2 serialized proposal options 2"),
			Data:            []byte("2 serialized proposal data 2"),
			Memo:            []byte("2 serialized proposal memo 2"),
			Status:          *queryParams.ProposalStatus,
		},
		{ // 2
			ID:              "3333333333333333333333333333333333333333333333333",
			ProposerAddr:    "333333333333333333333333333333333",
			StartTime:       queryParams.MinStartTime.Add(-time.Second), // starttime is before
			EndTime:         queryParams.MinStartTime.Add(time.Hour),
			Type:            *queryParams.ProposalType,
			IsAdminProposal: false,
			SerializedBytes: []byte("3 serialized proposal bytes 3"),
			Options:         []byte("3 serialized proposal options 3"),
			Data:            []byte("3 serialized proposal data 3"),
			Memo:            []byte("3 serialized proposal memo 3"),
			Status:          *queryParams.ProposalStatus,
		},
		{ // 3
			ID:              "4444444444444444444444444444444444444444444444444",
			ProposerAddr:    "444444444444444444444444444444444",
			StartTime:       queryParams.MaxStartTime.Add(time.Second), // starttime is after
			EndTime:         queryParams.MaxStartTime.Add(time.Hour),
			Type:            *queryParams.ProposalType,
			IsAdminProposal: false,
			SerializedBytes: []byte("4 serialized proposal bytes 4"),
			Options:         []byte("4 serialized proposal options 4"),
			Data:            []byte("4 serialized proposal data 4"),
			Memo:            []byte("4 serialized proposal memo 4"),
			Status:          *queryParams.ProposalStatus,
		},
		{ // 4 // cut by offset
			ID:              "5555555555555555555555555555555555555555555555555",
			ProposerAddr:    "555555555555555555555555555555555",
			StartTime:       queryParams.MinStartTime,
			EndTime:         queryParams.MinStartTime.Add(time.Hour),
			Type:            *queryParams.ProposalType,
			IsAdminProposal: false,
			SerializedBytes: []byte("5 serialized proposal bytes 5"),
			Options:         []byte("5 serialized proposal options 5"),
			Data:            []byte("5 serialized proposal data 5"),
			Memo:            []byte("5 serialized proposal memo 5"),
			Status:          *queryParams.ProposalStatus,
		},
		{ // 5
			ID:              "6666666666666666666666666666666666666666666666666",
			ProposerAddr:    "666666666666666666666666666666666",
			StartTime:       queryParams.MinStartTime,
			EndTime:         queryParams.MinStartTime.Add(time.Hour),
			Type:            *queryParams.ProposalType,
			SerializedBytes: []byte("6 serialized proposal bytes 6"),
			Options:         []byte("6 serialized proposal options 6"),
			Data:            []byte("6 serialized proposal data 6"),
			Memo:            []byte("6 serialized proposal memo 6"),
			Status:          *queryParams.ProposalStatus,
		},
		{ // 6
			ID:              "7777777777777777777777777777777777777777777777777",
			ProposerAddr:    "777777777777777777777777777777777",
			StartTime:       queryParams.MinStartTime,
			EndTime:         queryParams.MinStartTime.Add(time.Hour),
			Type:            *queryParams.ProposalType,
			IsAdminProposal: false,
			SerializedBytes: []byte("7 serialized proposal bytes 7"),
			Options:         []byte("7 serialized proposal options 7"),
			Data:            []byte("7 serialized proposal data 7"),
			Memo:            []byte("7 serialized proposal memo 7"),
			Status:          *queryParams.ProposalStatus,
		},
		{ // 7
			ID:              "8888888888888888888888888888888888888888888888888",
			ProposerAddr:    "888888888888888888888888888888888",
			StartTime:       queryParams.MinStartTime,
			EndTime:         queryParams.MinStartTime.Add(time.Hour),
			Type:            *queryParams.ProposalType,
			IsAdminProposal: true,
			SerializedBytes: []byte("8 serialized proposal bytes 8"),
			Options:         []byte("8 serialized proposal options 8"),
			Data:            []byte("8 serialized proposal data 8"),
			Memo:            []byte("8 serialized proposal memo 8"),
			Status:          *queryParams.ProposalStatus,
		},
		{ // 8 // cut by limit
			ID:              "9999999999999999999999999999999999999999999999999",
			ProposerAddr:    "999999999999999999999999999999999",
			StartTime:       queryParams.MinStartTime,
			EndTime:         queryParams.MinStartTime.Add(time.Hour),
			Type:            *queryParams.ProposalType,
			IsAdminProposal: false,
			SerializedBytes: []byte("9 serialized proposal bytes 9"),
			Options:         []byte("9 serialized proposal options 9"),
			Data:            []byte("9 serialized proposal data 9"),
			Memo:            []byte("9 serialized proposal memo 9"),
			Status:          *queryParams.ProposalStatus,
		},
		{ // 9
			ID:              "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
			ProposerAddr:    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
			StartTime:       queryParams.MinStartTime,
			EndTime:         queryParams.MinStartTime.Add(time.Hour),
			Type:            *queryParams.ProposalType,
			IsAdminProposal: true,
			SerializedBytes: []byte("A serialized proposal bytes A"),
			Options:         []byte("A serialized proposal options A"),
			Data:            []byte("A serialized proposal data A"),
			Memo:            []byte("A serialized proposal memo A"),
			Status:          models.ProposalStatusFailed, // different proposal status, but will be included in "completed" status query
		},
	}

	for _, proposal := range proposals {
		require.NoError(t, p.InsertTransactions(ctx, rawDBConn.NewSession(stream), &Transactions{
			ID:        proposal.ID,
			Memo:      proposal.Memo,
			CreatedAt: now,
		}, false))
		require.NoError(t, p.InsertDACProposal(ctx, rawDBConn.NewSession(stream), proposal))
	}

	resultProposals, err := p.QueryDACProposals(ctx, rawDBConn.NewSession(stream), queryParams)
	require.NoError(t, err)
	require.Equal(t, []DACProposal{*proposals[5], *proposals[6], *proposals[7]}, resultProposals)

	proposalStatusCompleted := models.ProposalStatusCompleted
	queryParams = &params.ListDACProposalsParams{
		ProposalStatus: &proposalStatusCompleted,
	}
	resultProposals, err = p.QueryDACProposals(ctx, rawDBConn.NewSession(stream), queryParams)
	require.NoError(t, err)
	require.Equal(t,
		[]DACProposal{*proposals[1], *proposals[2], *proposals[3], *proposals[4], *proposals[5], *proposals[6], *proposals[7], *proposals[8], *proposals[9]},
		resultProposals)
}

// TestInsertDACVote also tests QueryDACProposalVotes
func TestInsertDACVote(t *testing.T) {
	p := NewPersist()
	ctx := context.Background()
	stream := &dbr.NullEventReceiver{}
	rawDBConn, err := dbr.Open(TestDB, TestDSN, stream)
	require.NoError(t, err)
	_, err = rawDBConn.NewSession(stream).DeleteFrom(TableTransactions).Exec()
	require.NoError(t, err)
	_, err = rawDBConn.NewSession(stream).DeleteFrom(TableDACVotes).Exec()
	require.NoError(t, err)
	now := time.Now().UTC().Truncate(time.Second)

	vote := DACVote{
		VoteTxID:     "1111111111111111111111111111111111111111111111111",
		VoterAddr:    "222222222222222222222222222222222",
		VotedAt:      now,
		ProposalID:   "3333333333333333333333333333333333333333333333333",
		VotedOptions: []byte("serialized voted options"),
	}

	require.NoError(t, p.InsertDACVote(ctx, rawDBConn.NewSession(stream), &vote))

	expectedVote := vote
	expectedVote.ProposalID = ""
	resultVotes, err := p.QueryDACProposalVotes(ctx, rawDBConn.NewSession(stream), vote.ProposalID)
	require.NoError(t, err)
	require.Equal(t, []DACVote{expectedVote}, resultVotes)
}
