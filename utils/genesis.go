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

package utils

import (
	"context"
	"fmt"

	"github.com/ava-labs/avalanchego/api/info"
	"github.com/ava-labs/avalanchego/genesis"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/constants"
	platformGenesis "github.com/ava-labs/avalanchego/vms/platformvm/genesis"
	"github.com/ava-labs/avalanchego/vms/platformvm/txs"
	"github.com/chain4travel/magellan/cfg"
)

type GenesisContainer struct {
	NetworkID       uint32
	Time            uint64
	XChainGenesisTx *txs.Tx
	XChainID        ids.ID
	AvaxAssetID     ids.ID
	GenesisBytes    []byte
	Genesis         *platformGenesis.Genesis
}

func NewGenesisContainer(cfg *cfg.Config) (*GenesisContainer, error) {
	infoClient := info.NewClient(cfg.CaminoNode)

	gc := &GenesisContainer{NetworkID: cfg.NetworkID}
	genesisBytes, err := infoClient.GetGenesisBytes(context.Background())
	if err != nil {
		return nil, err
	}
	gc.GenesisBytes = genesisBytes

	gc.Genesis, err = platformGenesis.Parse(genesisBytes)
	if err != nil {
		return nil, err
	}
	for _, chain := range gc.Genesis.Chains {
		uChain := chain.Unsigned.(*txs.CreateChainTx)
		if uChain.VMID == constants.AVMID {
			gc.XChainGenesisTx = chain
			break
		}
	}
	if gc.XChainGenesisTx == nil {
		return nil, fmt.Errorf("couldn't find avm blockchain")
	}
	gc.AvaxAssetID, err = genesis.AVAXAssetID(gc.XChainGenesisTx.Unsigned.(*txs.CreateChainTx).GenesisData)
	if err != nil {
		return nil, err
	}
	gc.XChainID = gc.XChainGenesisTx.ID()

	gc.Time = gc.Genesis.Timestamp
	return gc, nil
}

func NewInternalGenesisContainer(networkID uint32) (*GenesisContainer, error) {
	gc := &GenesisContainer{NetworkID: networkID}
	config := genesis.GetConfig(gc.NetworkID)
	genesisBytes, avaxAssetID, err := genesis.FromConfig(config)
	if err != nil {
		return nil, err
	}
	gc.AvaxAssetID = avaxAssetID

	gc.Genesis, err = platformGenesis.Parse(genesisBytes)
	if err != nil {
		return nil, err
	}
	for _, chain := range gc.Genesis.Chains {
		uChain := chain.Unsigned.(*txs.CreateChainTx)
		if uChain.VMID == constants.AVMID {
			gc.XChainGenesisTx = chain
			break
		}
	}
	if gc.XChainGenesisTx == nil {
		return nil, fmt.Errorf("couldn't find avm blockchain")
	}
	gc.XChainID = gc.XChainGenesisTx.ID()
	gc.Time = config.StartTime
	return gc, nil
}
