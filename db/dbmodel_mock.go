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
	"sync"
	"time"

	"github.com/chain4travel/magellan/models"
	"github.com/chain4travel/magellan/services/indexes/params"
	"github.com/gocraft/dbr/v2"
)

type MockPersist struct {
	lock                           sync.RWMutex
	Transactions                   map[string]*Transactions
	Outputs                        map[string]*Outputs
	OutputsRedeeming               map[string]*OutputsRedeeming
	CvmTransactionsAtomic          map[string]*CvmTransactionsAtomic
	CvmTransactionsTxdata          map[string]*CvmTransactionsTxdata
	CvmAccounts                    map[string]*CvmAccount
	CvmBlocks                      map[string]*CvmBlocks
	CamLastBlockCache              map[string]*CamLastBlockCache
	CountLastBlockCache            map[string]*CountLastBlockCache
	CvmAddresses                   map[string]*CvmAddresses
	TransactionsValidator          map[string]*TransactionsValidator
	TransactionsBlock              map[string]*TransactionsBlock
	Addresses                      map[string]*Addresses
	AddressChain                   map[string]*AddressChain
	OutputAddresses                map[string]*OutputAddresses
	Assets                         map[string]*Assets
	TransactionsEpoch              map[string]*TransactionsEpoch
	PvmBlocks                      map[string]*PvmBlocks
	AddressBech32                  map[string]*AddressBech32
	OutputAddressAccumulateOut     map[string]*OutputAddressAccumulate
	OutputAddressAccumulateIn      map[string]*OutputAddressAccumulate
	OutputTxsAccumulate            map[string]*OutputTxsAccumulate
	AccumulateBalancesReceived     map[string]*AccumulateBalancesAmount
	AccumulateBalancesSent         map[string]*AccumulateBalancesAmount
	AccumulateBalancesTransactions map[string]*AccumulateBalancesTransactions
	TxPool                         map[string]*TxPool
	KeyValueStore                  map[string]*KeyValueStore
	NodeIndex                      map[string]*NodeIndex
	MultisigAlias                  map[string]*MultisigAlias
	RewardOwner                    map[string]*RewardOwner
	Reward                         map[string]*Reward
	DACProposals                   map[string]*DACProposal
	DACVotes                       map[string]*DACVote
}

func NewPersistMock() *MockPersist {
	return &MockPersist{
		Transactions:                   make(map[string]*Transactions),
		Outputs:                        make(map[string]*Outputs),
		OutputsRedeeming:               make(map[string]*OutputsRedeeming),
		CvmTransactionsAtomic:          make(map[string]*CvmTransactionsAtomic),
		CvmTransactionsTxdata:          make(map[string]*CvmTransactionsTxdata),
		CvmAccounts:                    make(map[string]*CvmAccount),
		CvmBlocks:                      make(map[string]*CvmBlocks),
		CamLastBlockCache:              make(map[string]*CamLastBlockCache),
		CvmAddresses:                   make(map[string]*CvmAddresses),
		TransactionsValidator:          make(map[string]*TransactionsValidator),
		TransactionsBlock:              make(map[string]*TransactionsBlock),
		Addresses:                      make(map[string]*Addresses),
		AddressChain:                   make(map[string]*AddressChain),
		OutputAddresses:                make(map[string]*OutputAddresses),
		Assets:                         make(map[string]*Assets),
		TransactionsEpoch:              make(map[string]*TransactionsEpoch),
		PvmBlocks:                      make(map[string]*PvmBlocks),
		AddressBech32:                  make(map[string]*AddressBech32),
		OutputAddressAccumulateOut:     make(map[string]*OutputAddressAccumulate),
		OutputAddressAccumulateIn:      make(map[string]*OutputAddressAccumulate),
		OutputTxsAccumulate:            make(map[string]*OutputTxsAccumulate),
		AccumulateBalancesReceived:     make(map[string]*AccumulateBalancesAmount),
		AccumulateBalancesSent:         make(map[string]*AccumulateBalancesAmount),
		AccumulateBalancesTransactions: make(map[string]*AccumulateBalancesTransactions),
		TxPool:                         make(map[string]*TxPool),
		KeyValueStore:                  make(map[string]*KeyValueStore),
		NodeIndex:                      make(map[string]*NodeIndex),
		MultisigAlias:                  make(map[string]*MultisigAlias),
		DACProposals:                   make(map[string]*DACProposal),
		DACVotes:                       make(map[string]*DACVote),
	}
}

func (m *MockPersist) QueryTransactions(ctx context.Context, runner dbr.SessionRunner, v *Transactions) (*Transactions, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if v, present := m.Transactions[v.ID]; present {
		return v, nil
	}
	return nil, nil
}

func (m *MockPersist) QueryMultipleTransactions(ctx context.Context, runner dbr.SessionRunner, txIDs []string) (*[]Transactions, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	var txs []Transactions
	for _, txID := range txIDs {
		if tx, ok := m.Transactions[txID]; ok {
			txs = append(txs, *tx)
		}
	}
	return &txs, nil
}

func (m *MockPersist) InsertTransactions(ctx context.Context, runner dbr.SessionRunner, v *Transactions, b bool) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	nv := &Transactions{}
	*nv = *v
	m.Transactions[v.ID] = nv
	return nil
}

func (m *MockPersist) QueryOutputsRedeeming(ctx context.Context, runner dbr.SessionRunner, v *OutputsRedeeming) (*OutputsRedeeming, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if v, present := m.OutputsRedeeming[v.ID]; present {
		return v, nil
	}
	return nil, nil
}

func (m *MockPersist) InsertOutputsRedeeming(ctx context.Context, runner dbr.SessionRunner, v *OutputsRedeeming, b bool) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	nv := &OutputsRedeeming{}
	*nv = *v
	m.OutputsRedeeming[v.ID] = nv
	return nil
}

func (m *MockPersist) QueryOutputs(ctx context.Context, runner dbr.SessionRunner, v *Outputs) (*Outputs, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if v, present := m.Outputs[v.ID]; present {
		return v, nil
	}
	return nil, nil
}

func (m *MockPersist) InsertOutputs(ctx context.Context, runner dbr.SessionRunner, v *Outputs, b bool) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	nv := &Outputs{}
	*nv = *v
	m.Outputs[v.ID] = nv
	return nil
}

func (m *MockPersist) QueryAssets(ctx context.Context, runner dbr.SessionRunner, v *Assets) (*Assets, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if v, present := m.Assets[v.ID]; present {
		return v, nil
	}
	return nil, nil
}

func (m *MockPersist) InsertAssets(ctx context.Context, runner dbr.SessionRunner, v *Assets, b bool) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	nv := &Assets{}
	*nv = *v
	m.Assets[v.ID] = nv
	return nil
}

func (m *MockPersist) QueryAddresses(ctx context.Context, runner dbr.SessionRunner, v *Addresses) (*Addresses, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if v, present := m.Addresses[v.Address]; present {
		return v, nil
	}
	return nil, nil
}

func (m *MockPersist) InsertAddresses(ctx context.Context, runner dbr.SessionRunner, v *Addresses, b bool) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	nv := &Addresses{}
	*nv = *v
	m.Addresses[v.Address] = nv
	return nil
}

func (m *MockPersist) QueryAddressChain(ctx context.Context, runner dbr.SessionRunner, v *AddressChain) (*AddressChain, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if v, present := m.AddressChain[v.Address+":"+v.ChainID]; present {
		return v, nil
	}
	return nil, nil
}

func (m *MockPersist) InsertAddressChain(ctx context.Context, runner dbr.SessionRunner, v *AddressChain, b bool) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	nv := &AddressChain{}
	*nv = *v
	m.AddressChain[v.Address+":"+v.ChainID] = nv
	return nil
}

func (m *MockPersist) QueryOutputAddresses(ctx context.Context, runner dbr.SessionRunner, v *OutputAddresses) (*OutputAddresses, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if v, present := m.OutputAddresses[v.OutputID+":"+v.Address]; present {
		return v, nil
	}
	return nil, nil
}

func (m *MockPersist) InsertOutputAddresses(ctx context.Context, runner dbr.SessionRunner, v *OutputAddresses, b bool) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	nv := &OutputAddresses{}
	*nv = *v
	m.OutputAddresses[v.OutputID+":"+v.Address] = nv
	return nil
}

func (m *MockPersist) UpdateOutputAddresses(ctx context.Context, runner dbr.SessionRunner, v *OutputAddresses) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	if fv, present := m.OutputAddresses[v.OutputID+":"+v.Address]; present {
		fv.RedeemingSignature = v.RedeemingSignature
	}
	return nil
}

func (m *MockPersist) QueryTransactionsEpoch(ctx context.Context, runner dbr.SessionRunner, v *TransactionsEpoch) (*TransactionsEpoch, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if v, present := m.TransactionsEpoch[v.ID]; present {
		return v, nil
	}
	return nil, nil
}

func (m *MockPersist) InsertTransactionsEpoch(ctx context.Context, runner dbr.SessionRunner, v *TransactionsEpoch, b bool) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	nv := &TransactionsEpoch{}
	*nv = *v
	m.TransactionsEpoch[v.ID] = nv
	return nil
}

func (m *MockPersist) QueryCvmBlock(ctx context.Context, runner dbr.SessionRunner, v *CvmBlocks) (*CvmBlocks, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if v, present := m.CvmBlocks[v.Block]; present {
		return v, nil
	}
	return nil, nil
}

func (m *MockPersist) InsertCvmBlocks(ctx context.Context, runner dbr.SessionRunner, v *CvmBlocks) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	nv := &CvmBlocks{}
	*nv = *v
	m.CvmBlocks[v.Block] = nv
	return nil
}

// this mock needs to be enriched
func (m *MockPersist) QueryCountLastBlockCache(ctx context.Context, runner dbr.SessionRunner, v *CamLastBlockCache) (*CountLastBlockCache, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	if _, present := m.CamLastBlockCache[v.ChainID]; present {
		cnt := &CountLastBlockCache{
			Cnt: 1,
		}
		return cnt, nil
	}
	return &CountLastBlockCache{}, nil
}

func (m *MockPersist) QueryCamLastBlockCache(ctx context.Context, runner dbr.SessionRunner, v *CamLastBlockCache) (*CamLastBlockCache, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if v, present := m.CamLastBlockCache[v.ChainID]; present {
		return v, nil
	}
	return nil, nil
}

func (m *MockPersist) InsertCamLastBlockCache(ctx context.Context, runner dbr.SessionRunner, v *CamLastBlockCache, flag bool) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	nv := &CamLastBlockCache{}
	*nv = *v
	m.CamLastBlockCache[v.CurrentBlock] = nv
	return nil
}

func (m *MockPersist) QueryCvmAddresses(ctx context.Context, runner dbr.SessionRunner, v *CvmAddresses) (*CvmAddresses, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if v, present := m.CvmAddresses[v.ID]; present {
		return v, nil
	}
	return nil, nil
}

func (m *MockPersist) InsertCvmAddresses(ctx context.Context, runner dbr.SessionRunner, v *CvmAddresses, b bool) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	nv := &CvmAddresses{}
	*nv = *v
	m.CvmAddresses[v.ID] = nv
	return nil
}

func (m *MockPersist) QueryCvmTransactionsAtomic(ctx context.Context, runner dbr.SessionRunner, v *CvmTransactionsAtomic) (*CvmTransactionsAtomic, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if v, present := m.CvmTransactionsAtomic[v.TransactionID]; present {
		return v, nil
	}
	return nil, nil
}

func (m *MockPersist) InsertCvmTransactionsAtomic(ctx context.Context, runner dbr.SessionRunner, v *CvmTransactionsAtomic, b bool) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	nv := &CvmTransactionsAtomic{}
	*nv = *v
	m.CvmTransactionsAtomic[v.TransactionID] = nv
	return nil
}

func (m *MockPersist) QueryCvmTransactionsTxdata(ctx context.Context, runner dbr.SessionRunner, v *CvmTransactionsTxdata) (*CvmTransactionsTxdata, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if v, present := m.CvmTransactionsTxdata[v.Hash]; present {
		return v, nil
	}
	return nil, nil
}

func (m *MockPersist) InsertCvmTransactionsTxdata(ctx context.Context, runner dbr.SessionRunner, v *CvmTransactionsTxdata, b bool) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	nv := &CvmTransactionsTxdata{}
	*nv = *v
	m.CvmTransactionsTxdata[v.Hash] = nv
	return nil
}

func (m *MockPersist) QueryCvmAccount(ctx context.Context, runner dbr.SessionRunner, v *CvmAccount) (*CvmAccount, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if v, present := m.CvmAccounts[v.Address]; present {
		return v, nil
	}
	return nil, nil
}

func (m *MockPersist) InsertCvmAccount(ctx context.Context, runner dbr.SessionRunner, v *CvmAccount, b bool) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	nv := &CvmAccount{}
	*nv = *v
	m.CvmAccounts[v.Address] = nv
	return nil
}

func (m *MockPersist) QueryPvmBlocks(ctx context.Context, runner dbr.SessionRunner, v *PvmBlocks) (*PvmBlocks, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if v, present := m.PvmBlocks[v.ID]; present {
		return v, nil
	}
	return nil, nil
}

func (m *MockPersist) InsertPvmBlocks(ctx context.Context, runner dbr.SessionRunner, v *PvmBlocks, b bool) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	nv := &PvmBlocks{}
	*nv = *v
	m.PvmBlocks[v.ID] = nv
	return nil
}

func (m *MockPersist) QueryTransactionsValidator(ctx context.Context, runner dbr.SessionRunner, v *TransactionsValidator) (*TransactionsValidator, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if v, present := m.TransactionsValidator[v.ID]; present {
		return v, nil
	}
	return nil, nil
}

func (m *MockPersist) InsertTransactionsValidator(ctx context.Context, runner dbr.SessionRunner, v *TransactionsValidator, b bool) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	nv := &TransactionsValidator{}
	*nv = *v
	m.TransactionsValidator[v.ID] = nv
	return nil
}

func (m *MockPersist) QueryTransactionsBlock(ctx context.Context, runner dbr.SessionRunner, v *TransactionsBlock) (*TransactionsBlock, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if v, present := m.TransactionsBlock[v.ID]; present {
		return v, nil
	}
	return nil, nil
}

func (m *MockPersist) InsertTransactionsBlock(ctx context.Context, runner dbr.SessionRunner, v *TransactionsBlock, b bool) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	nv := &TransactionsBlock{}
	*nv = *v
	m.TransactionsBlock[v.ID] = nv
	return nil
}

func (m *MockPersist) QueryAddressBech32(ctx context.Context, runner dbr.SessionRunner, v *AddressBech32) (*AddressBech32, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if v, present := m.AddressBech32[v.Address]; present {
		return v, nil
	}
	return nil, nil
}

func (m *MockPersist) InsertAddressBech32(ctx context.Context, runner dbr.SessionRunner, v *AddressBech32, b bool) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	nv := &AddressBech32{}
	*nv = *v
	m.AddressBech32[v.Address] = nv
	return nil
}

func (m *MockPersist) QueryOutputAddressAccumulateOut(ctx context.Context, runner dbr.SessionRunner, v *OutputAddressAccumulate) (*OutputAddressAccumulate, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if v, present := m.OutputAddressAccumulateOut[v.ID]; present {
		return v, nil
	}
	return nil, nil
}

func (m *MockPersist) InsertOutputAddressAccumulateOut(ctx context.Context, runner dbr.SessionRunner, v *OutputAddressAccumulate, _ bool) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	nv := &OutputAddressAccumulate{}
	*nv = *v
	m.OutputAddressAccumulateOut[v.ID] = nv
	return nil
}

func (m *MockPersist) QueryOutputAddressAccumulateIn(ctx context.Context, runner dbr.SessionRunner, v *OutputAddressAccumulate) (*OutputAddressAccumulate, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if v, present := m.OutputAddressAccumulateIn[v.ID]; present {
		return v, nil
	}
	return nil, nil
}

func (m *MockPersist) InsertOutputAddressAccumulateIn(ctx context.Context, runner dbr.SessionRunner, v *OutputAddressAccumulate, _ bool) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	nv := &OutputAddressAccumulate{}
	*nv = *v
	m.OutputAddressAccumulateIn[v.ID] = nv
	return nil
}

func (m *MockPersist) UpdateOutputAddressAccumulateInOutputsProcessed(ctx context.Context, runner dbr.SessionRunner, id string) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	for _, v := range m.OutputAddressAccumulateIn {
		if v.OutputID == id {
			v.OutputProcessed = 1
		}
	}
	return nil
}

func (m *MockPersist) QueryOutputTxsAccumulate(ctx context.Context, runner dbr.SessionRunner, v *OutputTxsAccumulate) (*OutputTxsAccumulate, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if v, present := m.OutputTxsAccumulate[v.ID]; present {
		return v, nil
	}
	return nil, nil
}

func (m *MockPersist) InsertOutputTxsAccumulate(ctx context.Context, runner dbr.SessionRunner, v *OutputTxsAccumulate) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	nv := &OutputTxsAccumulate{}
	*nv = *v
	m.OutputTxsAccumulate[v.ID] = nv
	return nil
}

func (m *MockPersist) QueryAccumulateBalancesReceived(ctx context.Context, runner dbr.SessionRunner, v *AccumulateBalancesAmount) (*AccumulateBalancesAmount, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if v, present := m.AccumulateBalancesReceived[v.ID]; present {
		return v, nil
	}
	return nil, nil
}

func (m *MockPersist) InsertAccumulateBalancesReceived(ctx context.Context, runner dbr.SessionRunner, v *AccumulateBalancesAmount) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	nv := &AccumulateBalancesAmount{}
	*nv = *v
	m.AccumulateBalancesReceived[v.ID] = nv
	return nil
}

func (m *MockPersist) QueryAccumulateBalancesSent(ctx context.Context, runner dbr.SessionRunner, v *AccumulateBalancesAmount) (*AccumulateBalancesAmount, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if v, present := m.AccumulateBalancesSent[v.ID]; present {
		return v, nil
	}
	return nil, nil
}

func (m *MockPersist) InsertAccumulateBalancesSent(ctx context.Context, runner dbr.SessionRunner, v *AccumulateBalancesAmount) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	nv := &AccumulateBalancesAmount{}
	*nv = *v
	m.AccumulateBalancesSent[v.ID] = nv
	return nil
}

func (m *MockPersist) QueryAccumulateBalancesTransactions(ctx context.Context, runner dbr.SessionRunner, v *AccumulateBalancesTransactions) (*AccumulateBalancesTransactions, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if v, present := m.AccumulateBalancesTransactions[v.ID]; present {
		return v, nil
	}
	return nil, nil
}

func (m *MockPersist) InsertAccumulateBalancesTransactions(ctx context.Context, runner dbr.SessionRunner, v *AccumulateBalancesTransactions) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	nv := &AccumulateBalancesTransactions{}
	*nv = *v
	m.AccumulateBalancesTransactions[v.ID] = nv
	return nil
}

func (m *MockPersist) QueryTxPool(ctx context.Context, runner dbr.SessionRunner, v *TxPool) (*TxPool, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if v, present := m.TxPool[v.ID]; present {
		return v, nil
	}
	return nil, nil
}

func (m *MockPersist) InsertTxPool(ctx context.Context, runner dbr.SessionRunner, v *TxPool) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	nv := &TxPool{}
	*nv = *v
	m.TxPool[v.ID] = nv
	return nil
}

func (m *MockPersist) RemoveTxPool(ctx context.Context, runner dbr.SessionRunner, v *TxPool) error {
	m.lock.RLock()
	defer m.lock.RUnlock()
	delete(m.TxPool, v.ID)
	return nil
}

func (m *MockPersist) QueryKeyValueStore(ctx context.Context, runner dbr.SessionRunner, v *KeyValueStore) (*KeyValueStore, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if v, present := m.KeyValueStore[v.K]; present {
		return v, nil
	}
	return nil, nil
}

func (m *MockPersist) InsertKeyValueStore(ctx context.Context, runner dbr.SessionRunner, v *KeyValueStore) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	nv := &KeyValueStore{}
	*nv = *v
	m.KeyValueStore[v.K] = nv
	return nil
}

func (m *MockPersist) QueryNodeIndex(ctx context.Context, runner dbr.SessionRunner, v *NodeIndex) (*NodeIndex, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if v, present := m.NodeIndex[v.Topic]; present {
		return v, nil
	}
	return nil, nil
}

func (m *MockPersist) InsertNodeIndex(ctx context.Context, runner dbr.SessionRunner, v *NodeIndex, _ bool) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	nv := &NodeIndex{}
	*nv = *v
	m.NodeIndex[v.Topic] = nv
	return nil
}

func (m *MockPersist) UpdateNodeIndex(ctx context.Context, runner dbr.SessionRunner, v *NodeIndex) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	if fv, present := m.NodeIndex[v.Topic]; present {
		fv.Idx = v.Idx
	}
	return nil
}

func (m *MockPersist) InsertMultisigAlias(ctx context.Context, runner dbr.SessionRunner, alias *MultisigAlias) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	nv := &MultisigAlias{}
	*nv = *alias
	m.MultisigAlias[alias.Owner] = nv
	return nil
}

func (m *MockPersist) QueryMultisigAlias(ctx context.Context, runner dbr.SessionRunner, v string) (*[]MultisigAlias, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	if v, present := m.MultisigAlias[v]; present {
		return &[]MultisigAlias{*v}, nil
	}
	return nil, nil
}

func (m *MockPersist) QueryMultisigAliasesForOwners(ctx context.Context, runner dbr.SessionRunner, v []string) (*[]MultisigAlias, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	if v, present := m.MultisigAlias[v[0]]; present {
		return &[]MultisigAlias{{Bech32Address: v.Bech32Address}}, nil
	}
	return nil, nil
}

func (m *MockPersist) DeleteMultisigAlias(ctx context.Context, runner dbr.SessionRunner, s string) error {
	m.lock.RLock()
	defer m.lock.RUnlock()
	delete(m.MultisigAlias, s)
	return nil
}

func (m *MockPersist) InsertRewardOwner(ctx context.Context, runner dbr.SessionRunner, owner *RewardOwner) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	nv := &RewardOwner{}
	*nv = *owner
	m.RewardOwner[nv.Address] = nv
	return nil
}

func (m *MockPersist) InsertReward(ctx context.Context, session dbr.SessionRunner, reward *Reward) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	nv := &Reward{}
	*nv = *reward
	m.Reward[nv.RewardOwnerHash] = nv
	return nil
}

func (m *MockPersist) InsertDACProposal(ctx context.Context, session dbr.SessionRunner, proposal *DACProposal) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	nv := &DACProposal{}
	*nv = *proposal
	m.DACProposals[proposal.ID] = nv
	return nil
}

func (m *MockPersist) UpdateDACProposal(ctx context.Context, session dbr.SessionRunner, proposalID string, updatedProposal []byte) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	proposal, ok := m.DACProposals[proposalID]
	if !ok {
		return nil
	}
	proposal.SerializedBytes = updatedProposal
	return nil
}

// QueryDACProposals is not deterministic about return result, cause of random mapping order
func (m *MockPersist) QueryDACProposals(ctx context.Context, session dbr.SessionRunner, params *params.ListDACProposalsParams) ([]DACProposal, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	proposals := make([]DACProposal, 0, len(m.DACProposals))
	offset := 0
	if params.Offset != 0 {
		offset = params.Offset
	}

	for _, v := range m.DACProposals {
		if len(proposals) == params.Limit {
			break
		}

		if offset > 0 {
			offset--
			continue
		}

		if (!params.StartTimeProvided || !params.StartTime.Before(v.StartTime)) &&
			(!params.EndTimeProvided || !params.EndTime.After(v.EndTime)) &&
			(params.ProposalType == nil || *params.ProposalType == v.Type) &&
			(params.ProposalStatus == nil || *params.ProposalStatus == v.Status) {
			proposals = append(proposals, *v)
		}
	}
	return proposals, nil
}

func (m *MockPersist) FinishDACProposals(ctx context.Context, session dbr.SessionRunner, proposalIDs []string, finishedAt time.Time, proposalStatus models.ProposalStatus) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	for _, proposalID := range proposalIDs {
		if proposal, ok := m.DACProposals[proposalID]; ok {
			proposal.FinishedAt = &finishedAt
			proposal.Status = proposalStatus
		}
	}
	return nil
}

func (m *MockPersist) FinishDACProposalWithOutcome(ctx context.Context, session dbr.SessionRunner, proposalID string, finishedAt time.Time, proposalStatus models.ProposalStatus, outcome []byte) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	proposal, ok := m.DACProposals[proposalID]
	if !ok {
		return nil
	}
	proposal.FinishedAt = &finishedAt
	proposal.Status = proposalStatus
	proposal.Outcome = outcome
	return nil
}

func (m *MockPersist) GetDACProposals(ctx context.Context, session dbr.SessionRunner, proposalIDs []string) ([]DACProposal, error) {
	proposals := make([]DACProposal, 0, len(proposalIDs))
	for i := range proposalIDs {
		if proposal, ok := m.DACProposals[proposalIDs[i]]; ok {
			proposals = append(proposals, *proposal)
		}
	}
	return proposals, nil
}

func (m *MockPersist) InsertDACVote(ctx context.Context, session dbr.SessionRunner, vote *DACVote) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	nv := &DACVote{}
	*nv = *vote
	m.DACVotes[vote.VoterAddr] = nv
	return nil
}

func (m *MockPersist) QueryDACProposalVotes(ctx context.Context, session dbr.SessionRunner, voterAddr string) ([]DACVote, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	votes := make([]DACVote, 0, len(m.DACVotes))
	for _, v := range m.DACVotes {
		if v.VoterAddr == voterAddr {
			votes = append(votes, *v)
		}
	}
	return votes, nil
}

func (m *MockPersist) GetTxHeight(ctx context.Context, session dbr.SessionRunner, txID string) (uint64, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	txBlock, ok := m.TransactionsBlock[txID]
	if !ok {
		return 0, dbr.ErrNotFound
	}
	pvmBlock, ok := m.PvmBlocks[txBlock.TxBlockID]
	if !ok {
		return 0, dbr.ErrNotFound
	}
	return pvmBlock.Height, nil
}
