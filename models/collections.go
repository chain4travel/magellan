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

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/math"
	"github.com/chain4travel/magellan/modelsc"
)

type MultisigAliasList struct {
	Alias []string `json:"alias"`
}

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

type EmissionsResult struct {
	Chain string  `json:"chain"`
	Time  string  `json:"time"`
	Value float64 `json:"value"`
}

type Emissions struct {
	Name   string      `json:"name"`
	Filter string      `jsont:"filter"`
	Value  interface{} `json:"value"`
}

type CountryEmissionsResult struct {
	Country string  `json:"Country"`
	Value   float64 `json:"Value"`
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

type CResult struct {
	Number uint64 `json:"number"`
	Hash   string `json:"hash"`
}

type StatisticsStruct struct {
	HighestNumber float64     `json:"highestValue"`
	HighestDate   string      `json:"highestDate"`
	LowestNumber  float64     `json:"lowestValue"`
	LowestDate    string      `json:"lowestDate"`
	TxInfo        interface{} `json:"txInfo"`
}

type TransactionsInfo struct {
	Date              string  `json:"date"`
	TotalTransactions int     `json:"totalTransactions"`
	AvgBlockTime      float32 `json:"avgBlockTime"`
	AvgBlockSize      float32 `json:"avgBlockSize"`
	TotalBlockCount   int     `json:"totalBlockCount"`
	TotalUnclesCount  int     `json:"totalUnclesCount"`
	NewAddressSeen    string  `json:"newAddressSeen"`
}

type TransactionsPerDate struct {
	Counter float64 `json:"counter"`
	DateAt  string  `json:"dateAt"`
}

type GasUsedPerDate struct {
	Gas  float32 `json:"avgGas"`
	Date string  `json:"date"`
}

type AverageBlockSize struct {
	BlockSize float64 `json:"blockSize"`
	DateInfo  string  `json:"dateInfo"`
}

type AddressStruct struct {
	HighestNumber int         `json:"highestValue"`
	HighestDate   string      `json:"highestDate"`
	LowestNumber  int         `json:"lowestValue"`
	LowestDate    string      `json:"lowestDate"`
	AddressInfo   interface{} `json:"addressInfo"`
}
type UniqueAddresses struct {
	TotalAddresses int    `json:"totalAddresses"`
	DateAt         string `json:"dateAt"`
	DailyIncrease  int    `json:"dailyIncrease"`
}

type ActiveAddresses struct {
	Total        int    `json:"total"`
	ReceiveCount int    `json:"receiveCount"`
	SendCount    int    `json:"sendCount"`
	DateAt       string `json:"dateAt"`
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
	AggregateMerge
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
	AggregateMerge
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

type BlockValue struct {
	Block uint64 `json:"block"`
}

type AddressChains struct {
	AddressChains map[string][]StringID `json:"addressChains"`
}

type AssetAggregate struct {
	Asset     ids.ID               `json:"asset"`
	Aggregate *AggregatesHistogram `json:"aggregate"`
}

type NetworkNameResponse struct {
	Jsonrpc string `json:"jsonrpc"`
	Result  struct {
		NetworkName string `json:"networkName"`
	} `json:"result"`
}

type IPAPIResponse struct {
	Country     string  `json:"country"`
	CountryCode string  `json:"countryCode"`
	City        string  `json:"city"`
	Lat         float64 `json:"lat"`
	Lon         float64 `json:"lon"`
}

type GeoIPValidators struct {
	Name  string       `json:"name"`
	Value []*Validator `json:"value"`
}

type Validator struct {
	NodeID     ids.NodeID `json:"nodeID"`
	TxID       ids.ID     `json:"txID"`
	Connected  bool       `json:"connected"`
	Uptime     float32    `json:"uptime"`
	Lng        float64    `json:"lng"`
	Lat        float64    `json:"lat"`
	IP         string     `json:"IP"`
	StartTime  string     `json:"startTime"`
	EndTime    string     `json:"endTime"`
	Duration   string     `json:"duration"`
	Country    string     `json:"country"`
	CountryISO string     `json:"countryISO"`
	City       string     `json:"city"`
}

type StatisticsCache struct {
	DateAt         string  `json:"dateAt"`
	CvmTx          int     `json:"cvmTx"`
	AvmTx          int     `json:"avmTx"`
	ReceiveCount   int     `json:"receiveCount"`
	SendCount      int     `json:"sendCount"`
	ActiveAccounts int     `json:"activeAccounts"`
	Blocks         int     `json:"blocks"`
	GasPrice       float32 `json:"gasPrice"`
	TokenTransfer  float32 `json:"tokenTransfer"`
	GasUsed        float32 `json:"gasUsed"`
	AvgBlockSize   float32 `json:"avgBlockSize"`
}

type CvmStatisticsCache struct {
	DateAt         string  `json:"dateAt"`
	CvmTx          int     `json:"cvmTx"`
	ReceiveCount   int     `json:"receiveCount"`
	SendCount      int     `json:"sendCount"`
	ActiveAccounts int     `json:"activeAccounts"`
	Blocks         int     `json:"blocks"`
	GasPrice       float32 `json:"gasPrice"`
	GasUsed        float32 `json:"gasUsed"`
	TokenTransfer  float32 `json:"tokenTransfer"`
}

type AvmStatisticsCache struct {
	DateAt string `json:"dateAt"`
	AvmTx  int    `json:"avmTx"`
}

type CvmBlocksStatisticsCache struct {
	DateAt       string  `json:"dateAt"`
	AvgBlockSize float32 `json:"avgBlockSize"`
}

type AddressesCache struct {
	Address string `json:"address"`
	DateAt  string `json:"dateAt"`
}

/*******************  Merging  ***********************/

type AggregateMerge interface {
	ID() int
	Merge(AggregateMerge)
}

type AggregateMergeList []AggregateMerge

func (a *Aggregates) ID() int { return a.IntervalID }

func (a *Aggregates) Merge(b AggregateMerge) {
	src := b.(*Aggregates)
	a.AddressCount += src.AddressCount
	a.AssetCount = math.Max(a.AssetCount, src.AssetCount)
	a.OutputCount += src.OutputCount
	a.TransactionCount += src.TransactionCount
	a.TransactionVolume += src.TransactionVolume
}

func (al AggregatesList) MergeList() *AggregateMergeList {
	result := make(AggregateMergeList, len(al))
	for i := range al {
		result[i] = &al[i]
	}
	return &result
}

func (a *TxfeeAggregates) ID() int { return a.IntervalID }

func (a *TxfeeAggregates) Merge(b AggregateMerge) {
	src := b.(*TxfeeAggregates)
	a.Txfee += src.Txfee
}

func (al TxfeeAggregatesList) MergeList() *AggregateMergeList {
	result := make(AggregateMergeList, len(al))
	for i := range al {
		result[i] = &al[i]
	}
	return &result
}

// Merges two TxfeeAggregateList, both have to be sorted by Idx
func MergeAggregates(dst, src *AggregateMergeList) {
	if len(*src) == 0 {
		return
	}

	merged := []AggregateMerge{}
	srcID := 0
	for _, dstI := range *dst {
		// Insert smallerLists from src
		for srcID < len(*src) && (*src)[srcID].ID() < dstI.ID() {
			merged = append(merged, (*src)[srcID])
			srcID++
		}
		// Insert dst elem
		merged = append(merged, dstI)
		// cummulate values if it's the same id
		if srcID < len(*src) && (*src)[srcID].ID() == dstI.ID() {
			merged[len(merged)-1].Merge((*src)[srcID])
			srcID++
		}
	}
	merged = append(merged, (*src)[srcID:]...)

	*dst = merged
}
