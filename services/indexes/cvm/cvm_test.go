// Copyright (C) 2022, Chain4Travel AG. All rights reserved.
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

package cvm

import (
	"context"
	"testing"
	"time"

	"github.com/chain4travel/caminoethvm/core/types"
	"github.com/chain4travel/caminoethvm/plugin/evm"
	"github.com/chain4travel/caminogo/ids"
	"github.com/chain4travel/caminogo/utils/logging"
	caminoGoAvax "github.com/chain4travel/caminogo/vms/components/avax"
	"github.com/chain4travel/caminogo/vms/secp256k1fx"
	"github.com/chain4travel/magellan/cfg"
	"github.com/chain4travel/magellan/db"
	"github.com/chain4travel/magellan/models"
	"github.com/chain4travel/magellan/services"
	"github.com/chain4travel/magellan/servicesctrl"
	"github.com/chain4travel/magellan/utils"
)

var testXChainID = ids.ID([32]byte{7, 193, 50, 215, 59, 55, 159, 112, 106, 206, 236, 110, 229, 14, 139, 125, 14, 101, 138, 65, 208, 44, 163, 38, 115, 182, 177, 179, 244, 34, 195, 120})

func newTestIndex(t *testing.T, networkID uint32, chainID ids.ID) (*utils.Connections, *Writer, func()) {
	logConf := logging.DefaultConfig

	conf := cfg.Services{
		Logging: logConf,
		DB: &cfg.DB{
			Driver: "mysql",
			DSN:    "root:password@tcp(127.0.0.1:3306)/magellan_test?parseTime=true",
		},
	}

	sc := &servicesctrl.Control{Log: logging.NoLog{}, Services: conf}
	conns, err := sc.Database()
	if err != nil {
		t.Fatal("Failed to create connections:", err.Error())
	}

	// Create index
	writer, err := NewWriter(networkID, chainID.String(), nil)
	if err != nil {
		t.Fatal("Failed to create writer:", err.Error())
	}

	return conns, writer, func() {
		_ = conns.Close()
	}
}

func TestInsertTxInternalExport(t *testing.T) {
	conns, writer, closeFn := newTestIndex(t, 5, testXChainID)
	defer closeFn()
	ctx := context.Background()

	tx := &evm.Tx{}

	extx := &evm.UnsignedExportTx{}
	extxIn := evm.EVMInput{}
	extx.Ins = []evm.EVMInput{extxIn}
	transferableOut := &caminoGoAvax.TransferableOutput{}
	transferableOut.Out = &secp256k1fx.TransferOutput{}
	extx.ExportedOutputs = []*caminoGoAvax.TransferableOutput{transferableOut}

	tx.UnsignedAtomicTx = extx
	block := types.NewBlock(
		&types.Header{},
		[]*types.Transaction{},
		[]*types.Header{},
		[]*types.Receipt{},
		nil,
		tx.Bytes(),
		false,
	)

	persist := db.NewPersistMock()
	session := conns.DB().NewSessionForEventReceiver(conns.Stream().NewJob("test_tx"))
	cCtx := services.NewConsumerContext(ctx, session, time.Now().Unix(), 0, persist, testXChainID.String())
	err := writer.indexBlockInternal(cCtx, []*evm.Tx{tx}, &models.BlockProposal{}, block)
	if err != nil {
		t.Fatal("insert failed", err)
	}
	if len(persist.CvmTransactionsAtomic) != 1 {
		t.Fatal("insert failed")
	}
	if len(persist.Outputs) != 1 {
		t.Fatal("insert failed")
	}
}

func TestInsertTxInternalImport(t *testing.T) {
	conns, writer, closeFn := newTestIndex(t, 5, testXChainID)
	defer closeFn()
	ctx := context.Background()
	tx := &evm.Tx{}

	extx := &evm.UnsignedImportTx{}
	evtxOut := evm.EVMOutput{}
	extx.Outs = []evm.EVMOutput{evtxOut}
	transferableIn := &caminoGoAvax.TransferableInput{}
	transferableIn.In = &secp256k1fx.TransferInput{}
	extx.ImportedInputs = []*caminoGoAvax.TransferableInput{transferableIn}

	tx.UnsignedAtomicTx = extx
	block := types.NewBlock(
		&types.Header{},
		[]*types.Transaction{},
		[]*types.Header{},
		[]*types.Receipt{},
		nil,
		tx.Bytes(),
		false,
	)

	persist := db.NewPersistMock()
	session := conns.DB().NewSessionForEventReceiver(conns.Stream().NewJob("test_tx"))
	cCtx := services.NewConsumerContext(ctx, session, time.Now().Unix(), 0, persist, testXChainID.String())
	err := writer.indexBlockInternal(cCtx, []*evm.Tx{tx}, &models.BlockProposal{}, block)
	if err != nil {
		t.Fatal("insert failed", err)
	}
	if len(persist.CvmTransactionsAtomic) != 1 {
		t.Fatal("insert failed")
	}
	if len(persist.CvmAddresses) != 1 {
		t.Fatal("insert failed")
	}
	if len(persist.OutputsRedeeming) != 1 {
		t.Fatal("insert failed")
	}
}
