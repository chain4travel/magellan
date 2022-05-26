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

package models

import (
	"time"

	"github.com/chain4travel/caminogo/ids"
	"github.com/chain4travel/magellan/modelsc"
)

type ListMetadata struct {
	Count *uint64 `json:"count,omitempty"`
}

type TransactionList struct {
	ListMetadata

	Transactions []*Transaction `json:"transactions"`

	// StartTime is the calculated start time rounded to the nearest
	// TransactionRoundDuration.
	StartTime time.Time `json:"startTime"`

	// EndTime is the calculated end time rounded to the nearest
	// TransactionRoundDuration.
	EndTime time.Time `json:"endTime"`

	Next *string `json:"next,omitempty"`
}

type CTransactionData struct {
	Type      int       `json:"type"`
	Block     string    `json:"block"`
	Hash      string    `json:"hash"`
	CreatedAt time.Time `json:"createdAt"`
	Nonce     uint64    `json:"nonce"`
	GasPrice  *string   `json:"gasPrice,omitempty"`
	GasFeeCap *string   `json:"maxFeePerGas,omitempty"`
	GasTipCap *string   `json:"maxPriorityFeePerGas,omitempty"`
	GasLimit  uint64    `json:"gasLimit"`
	Amount    *string   `json:"value,omitempty"`
	Payload   *string   `json:"input,omitempty"`
	FromAddr  string    `json:"fromAddr"`
	ToAddr    string    `json:"toAddr"`

	// Signature values
	V *string `json:"v,omitempty"`
	R *string `json:"r,omitempty"`
	S *string `json:"s,omitempty"`

	Receipt *modelsc.ExtendedReceipt `json:"receipt"`
}

type CBlockHeaderBase struct {
	Hash       string `json:"hash"`
	Coinbase   string `json:"miner"`
	Difficulty string `json:"difficulty"`
	Number     string `json:"number"`
	GasLimit   string `json:"gasLimit"`
	GasUsed    string `json:"gasUsed"`
	Time       string `json:"timestamp"`
	BaseFee    string `json:"baseFeePerGas"`

	ExtDataGasUsed string `json:"extDataGasUsed,omitempty"`
	BlockGasCost   string `json:"blockGasCost,omitempty"`

	EvmTx    int16 `json:"evmTx,omitempty"`
	AtomicTx int16 `json:"atomicTx,omitempty"`
}

type CTransactionDataBase struct {
	Type      string `json:"type"`
	Block     string `json:"block"`
	Index     string `json:"index"`
	Hash      string `json:"hash"`
	Nonce     string `json:"nonce"`
	GasPrice  string `json:"gasPrice,omitempty"`
	GasFeeCap string `json:"maxFeePerGas,omitempty"`
	GasTipCap string `json:"maxPriorityFeePerGas,omitempty"`
	Gas       string `json:"gas"`
	Amount    string `json:"value"`
	From      string `json:"from"`
	To        string `json:"to,omitempty"`

	CreatedAt         string `json:"timestamp"`
	Status            string `json:"status"`
	GasUsed           string `json:"gasUsed"`
	EffectiveGasPrice string `json:"effectiveGasPrice"`
}

type CBlockList struct {
	BlockCount       uint64 `json:"blockCount"`
	TransactionCount uint64 `json:"transactionCount"`

	Blocks       []*CBlockHeaderBase     `json:"blocks"`
	Transactions []*CTransactionDataBase `json:"transactions"`
}

type CTransactionList struct {
	Transactions []*CTransactionData
	// StartTime is the calculated start time rounded to the nearest
	// TransactionRoundDuration.
	StartTime time.Time `json:"startTime"`

	// EndTime is the calculated end time rounded to the nearest
	// TransactionRoundDuration.
	EndTime time.Time `json:"endTime"`
}

type AssetList struct {
	ListMetadata
	Assets []*Asset `json:"assets"`
}

type AddressList struct {
	ListMetadata
	Addresses []*AddressInfo `json:"addresses"`
}

// SearchResults represents a set of items returned for a search query.
type SearchResults struct {
	// Count is the total number of matching results
	Count uint64 `json:"count"`

	// Results is a list of SearchResult
	Results SearchResultSet `json:"results"`
}

type SearchResultSet []SearchResult

func (s SearchResultSet) Len() int           { return len(s) }
func (s SearchResultSet) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s SearchResultSet) Less(i, j int) bool { return s[i].Score < s[j].Score }

// SearchResult represents a single item matching a search query.
type SearchResult struct {
	// SearchResultType is the type of object found
	SearchResultType `json:"type"`

	// Data is the object itself
	Data interface{} `json:"data"`

	// Score is a rank of how well this result matches the query
	Score uint64 `json:"score"`
}

type TxfeeAggregatesHistogram struct {
	TxfeeAggregates TxfeeAggregates   `json:"aggregates"`
	IntervalSize    time.Duration     `json:"intervalSize,omitempty"`
	Intervals       []TxfeeAggregates `json:"intervals,omitempty"`

	// StartTime is the calculated start time rounded to the nearest
	// TransactionRoundDuration.
	StartTime time.Time `json:"startTime"`

	// EndTime is the calculated end time rounded to the nearest
	// TransactionRoundDuration.
	EndTime time.Time `json:"endTime"`
}

type TxfeeAggregates struct {
	// IntervalID is used internally when creating a histogram of Aggregates.
	// It is exported only so it can be written to by dbr.
	IntervalID int `json:"-"`

	// StartTime is the calculated start time rounded to the nearest
	// TransactionRoundDuration.
	StartTime time.Time `json:"startTime"`

	// EndTime is the calculated end time rounded to the nearest
	// TransactionRoundDuration.
	EndTime time.Time `json:"endTime"`

	Txfee uint64 `json:"txfee"`
}

type TxfeeAggregatesList []TxfeeAggregates

type AggregatesHistogram struct {
	Aggregates   Aggregates    `json:"aggregates"`
	IntervalSize time.Duration `json:"intervalSize,omitempty"`
	Intervals    []Aggregates  `json:"intervals,omitempty"`

	// StartTime is the calculated start time rounded to the nearest
	// TransactionRoundDuration.
	StartTime time.Time `json:"startTime"`

	// EndTime is the calculated end time rounded to the nearest
	// TransactionRoundDuration.
	EndTime time.Time `json:"endTime"`
}

type Aggregates struct {
	// IntervalID is used internally when creating a histogram of Aggregates.
	// It is exported only so it can be written to by dbr.
	IntervalID int `json:"-"`

	// StartTime is the calculated start time rounded to the nearest
	// TransactionRoundDuration.
	StartTime time.Time `json:"startTime"`

	// EndTime is the calculated end time rounded to the nearest
	// TransactionRoundDuration.
	EndTime time.Time `json:"endTime"`

	TransactionVolume TokenAmount `json:"transactionVolume"`
	TransactionCount  uint64      `json:"transactionCount"`
	AddressCount      uint64      `json:"addressCount"`
	OutputCount       uint64      `json:"outputCount"`
	AssetCount        uint64      `json:"assetCount"`
}

type AggregatesList []Aggregates

type AddressChains struct {
	AddressChains map[string][]StringID `json:"addressChains"`
}

type AssetAggregate struct {
	Asset     ids.ID               `json:"asset"`
	Aggregate *AggregatesHistogram `json:"aggregate"`
}

// Merges two TxfeeAggregateList, both have to be sorted by Idx
func (a *TxfeeAggregatesList) Merge(src TxfeeAggregatesList) {
	if len(src) == 0 {
		return
	}

	var merged TxfeeAggregatesList
	srcID := 0
	for _, dst := range *a {
		// Insert smallerLists from src
		for srcID < len(src) && src[srcID].IntervalID < dst.IntervalID {
			merged = append(merged, src[srcID])
			srcID++
		}
		// Insert dst elem
		merged = append(merged, dst)
		// cummulate values if it's the same id
		if srcID < len(src) && src[srcID].IntervalID == dst.IntervalID {
			last := len(merged) - 1
			merged[last].Txfee += src[srcID].Txfee
			srcID++
		}
	}
	merged = append(merged, src[srcID:]...)

	*a = merged
}

// Merges two AggregateList, both have to be sorted by Idx
func (a *AggregatesList) Merge(src AggregatesList) {
	if len(src) == 0 {
		return
	}

	var merged AggregatesList
	srcID := 0
	for _, dst := range *a {
		// Insert smallerLists from src
		for srcID < len(src) && src[srcID].IntervalID < dst.IntervalID {
			merged = append(merged, src[srcID])
			srcID++
		}
		// Insert dst elem
		merged = append(merged, dst)
		// cummulate values if it's the same id
		if srcID < len(src) && src[srcID].IntervalID == dst.IntervalID {
			last := len(merged) - 1
			merged[last].AddressCount += src[srcID].AddressCount
			merged[last].AssetCount += src[srcID].AssetCount
			merged[last].TransactionCount += src[srcID].TransactionCount
			merged[last].TransactionVolume += src[srcID].TransactionVolume
			srcID++
		}
	}
	merged = append(merged, src[srcID:]...)

	*a = merged
}
