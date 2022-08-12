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

package avax

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"sync"
	"time"

	corethType "github.com/chain4travel/caminoethvm/core/types"
	"github.com/chain4travel/caminogo/ids"
	"github.com/chain4travel/magellan/cfg"
	"github.com/chain4travel/magellan/db"
	"github.com/chain4travel/magellan/models"
	"github.com/chain4travel/magellan/services"
	"github.com/chain4travel/magellan/services/indexes/params"
	"github.com/chain4travel/magellan/servicesctrl"
	"github.com/chain4travel/magellan/utils"
	"github.com/gocraft/dbr/v2"
)

const (
	MaxAggregateIntervalCount = 20000

	MinSearchQueryLength = 3
)

var (
	ErrAggregateIntervalCountTooLarge = errors.New("requesting too many intervals")
	ErrFailedToParseStringAsBigInt    = errors.New("failed to parse string to big.Int")
	ErrSearchQueryTooShort            = errors.New("search query too short")

	outputSelectColumns = []string{
		"avm_outputs.id",
		"avm_outputs.transaction_id",
		"avm_outputs.output_index",
		"avm_outputs.asset_id",
		"avm_outputs.output_type",
		"avm_outputs.amount",
		"avm_outputs.locktime",
		"avm_outputs.threshold",
		"avm_outputs.created_at",
		"case when avm_outputs_redeeming.redeeming_transaction_id IS NULL then '' else avm_outputs_redeeming.redeeming_transaction_id end as redeeming_transaction_id",
		"avm_outputs.group_id",
		"avm_outputs.payload",
		"avm_outputs.frozen",
	}
)

type Reader struct {
	conns          *utils.Connections
	sc             *servicesctrl.Control
	avmLock        sync.RWMutex
	networkID      uint32
	chainConsumers map[string]services.Consumer

	readerAggregate ReaderAggregate

	doneCh chan struct{}
}

func NewReader(networkID uint32, conns *utils.Connections, chainConsumers map[string]services.Consumer, sc *servicesctrl.Control) (*Reader, error) {
	reader := &Reader{
		conns:          conns,
		sc:             sc,
		networkID:      networkID,
		chainConsumers: chainConsumers,
		doneCh:         make(chan struct{}),
	}

	err := reader.aggregateProcessor()
	if err != nil {
		return nil, err
	}

	return reader, nil
}

func (r *Reader) Search(ctx context.Context, p *params.SearchParams, avaxAssetID ids.ID) (*models.SearchResults, error) {
	p.ListParams.DisableCounting = true

	var cblocks []models.CResult
	if blockHeight, err := strconv.ParseInt(p.ListParams.Query, 10, 64); err == nil && blockHeight > 0 {
		cblocks, _ = r.searchCBlockHeight(ctx, uint64(blockHeight))
	}

	if len(p.ListParams.Query) < MinSearchQueryLength && cblocks == nil {
		return nil, ErrSearchQueryTooShort
	}

	// See if the query string is an id or shortID. If so we can search on them
	// directly. Otherwise we treat the query as a normal query-string.
	if shortID, err := params.AddressFromString(p.ListParams.Query); err == nil {
		return r.searchByShortID(ctx, shortID)
	}
	if id, err := ids.FromString(p.ListParams.Query); err == nil {
		return r.searchByID(ctx, id, avaxAssetID)
	}

	var ctrans []models.CResult
	var caddr []models.CResult
	// get all 0x based C-Chain results
	if cblocks == nil && strings.HasPrefix(p.ListParams.Query, "0x") {
		if len(p.ListParams.Query) == 66 { // block hash or tx hash
			cblocks, _ = r.searchCBlockHash(ctx, p.ListParams.Query)
			ctrans, _ = r.searchCTransHash(ctx, p.ListParams.Query)
		} else if len(p.ListParams.Query) == 42 { // address
			caddr, _ = r.searchCAddress(ctx, p.ListParams.Query)
		}
	}

	var assets []*models.Asset
	var txs []*models.Transaction
	var addresses []*models.AddressInfo

	lenSearchResults := func() int {
		return len(assets) + len(txs) + len(addresses) + len(cblocks) + len(ctrans) + len(caddr)
	}

	assetsResp, err := r.ListAssets(ctx, &params.ListAssetsParams{ListParams: p.ListParams}, nil)
	if err != nil {
		return nil, err
	}
	assets = assetsResp.Assets
	if lenSearchResults() >= p.ListParams.Limit {
		return collateSearchResults(assets, addresses, txs, cblocks, ctrans, caddr)
	}

	dbRunner, err := r.conns.DB().NewSession("search", cfg.RequestTimeout)
	if err != nil {
		return nil, err
	}

	builder1 := transactionQuery(dbRunner).
		Where(dbr.Like("avm_transactions.id", p.ListParams.Query+"%")).
		OrderDesc("avm_transactions.created_at").
		Limit(uint64(p.ListParams.Limit))
	if _, err := builder1.LoadContext(ctx, &txs); err != nil {
		return nil, err
	}
	if lenSearchResults() >= p.ListParams.Limit {
		return collateSearchResults(assets, addresses, txs, cblocks, ctrans, caddr)
	}

	return collateSearchResults(assets, addresses, txs, cblocks, ctrans, caddr)
}

func (r *Reader) TxfeeAggregate(ctx context.Context, params *params.TxfeeAggregateParams) (*models.TxfeeAggregatesHistogram, error) {
	// if the request is not coming from the caching mechanism then return the values of the cache and do NOT probe the database
	if !params.ListParams.Values.Has("cacheUpd") {
		cache := models.TxfeeAggregatesList{}
		var temp = models.TxfeeAggregates{}
		// based on the date interval we are going to retrieve the relevant part from our cache
		var keyDatePartValue = cfg.GetDatepartBasedOnDateParams(params.ListParams.StartTime, params.ListParams.EndTime)
		temp.Txfee = cfg.GetAggregateFeesMap()[params.ChainIDs[0]][keyDatePartValue]

		cache = append(cache, temp)
		cache[0].StartTime = params.ListParams.StartTime
		cache[0].EndTime = params.ListParams.EndTime
		return &models.TxfeeAggregatesHistogram{
			TxfeeAggregates: cache[0],
			StartTime:       params.ListParams.StartTime,
			EndTime:         params.ListParams.EndTime,
		}, nil
	}

	// Validate params and set defaults if necessary
	if params.ListParams.StartTime.IsZero() {
		var err error
		params.ListParams.StartTime, err = r.getFirstTransactionTime(ctx, params.ChainIDs)
		if err != nil {
			return nil, err
		}
	}

	intervals := models.TxfeeAggregatesList{}

	// Ensure the interval count requested isn't too large
	intervalSeconds := int64(params.IntervalSize.Seconds())
	requestedIntervalCount := 0
	if intervalSeconds != 0 {
		requestedIntervalCount = int(math.Ceil(params.ListParams.EndTime.Sub(params.ListParams.StartTime).Seconds() / params.IntervalSize.Seconds()))
		if requestedIntervalCount > MaxAggregateIntervalCount {
			return nil, ErrAggregateIntervalCountTooLarge
		}
		if requestedIntervalCount < 1 {
			requestedIntervalCount = 1
		}
	}

	// Split chains
	var avmChains, cvmChains []string
	if len(params.ChainIDs) == 0 {
		for id, chain := range r.sc.Chains {
			switch chain.VMType {
			case models.CVMName:
				cvmChains = append(cvmChains, id)
			default:
				avmChains = append(avmChains, id)
			}
		}
	} else {
		for _, id := range params.ChainIDs {
			chain, exist := r.sc.Chains[id]
			if exist {
				switch chain.VMType {
				case models.CVMName:
					cvmChains = append(cvmChains, id)
				default:
					avmChains = append(avmChains, id)
				}
			}
		}
	}

	// Build the query and load the base data
	dbRunner, err := r.conns.DB().NewSession("get_txfee_aggregates_histogram", cfg.RequestTimeout)
	if err != nil {
		return nil, err
	}

	var builder *dbr.SelectStmt

	if len(avmChains) > 0 {
		columns := []string{
			"CAST(COALESCE(SUM(avm_transactions.txfee), 0) AS UNSIGNED) AS txfee",
		}

		if requestedIntervalCount > 0 {
			columns = append(columns, fmt.Sprintf(
				"FLOOR((UNIX_TIMESTAMP(avm_transactions.created_at)-%d) / %d) AS interval_id",
				params.ListParams.StartTime.Unix(),
				intervalSeconds))
		}

		builder = dbRunner.
			Select(columns...).
			From("avm_transactions").
			Where("avm_transactions.created_at >= ?", params.ListParams.StartTime).
			Where("avm_transactions.created_at < ?", params.ListParams.EndTime)

		if requestedIntervalCount > 0 {
			builder.
				GroupBy("interval_id").
				OrderAsc("interval_id").
				Limit(uint64(requestedIntervalCount))
		}

		if len(params.ChainIDs) != 0 {
			builder.Where("avm_transactions.chain_id IN ?", params.ChainIDs)
		}

		_, err = builder.LoadContext(ctx, &intervals)
		if err != nil {
			return nil, err
		}
	}

	if len(cvmChains) > 0 {
		columns := []string{
			"CAST(COALESCE(SUM((cvm_transactions_txdata.gas_price / 1000000000) * cvm_transactions_txdata.gas_used), 0) AS UNSIGNED) AS txfee",
		}

		if requestedIntervalCount > 0 {
			columns = append(columns, fmt.Sprintf(
				"FLOOR((UNIX_TIMESTAMP(cvm_transactions_txdata.created_at)-%d) / %d) AS interval_id",
				params.ListParams.StartTime.Unix(),
				intervalSeconds))
		}

		builder = dbRunner.
			Select(columns...).
			From("cvm_transactions_txdata").
			Where("cvm_transactions_txdata.created_at >= ?", params.ListParams.StartTime).
			Where("cvm_transactions_txdata.created_at < ?", params.ListParams.EndTime)

		if requestedIntervalCount > 0 {
			builder.
				GroupBy("interval_id").
				OrderAsc("interval_id").
				Limit(uint64(requestedIntervalCount))
		}

		if len(params.ChainIDs) != 0 {
			builder.
				Join("cvm_blocks", "cvm_blocks.block = cvm_transactions_txdata.block").
				Where("cvm_blocks.chain_id IN ?", cvmChains)
		}

		cvmIntervals := models.TxfeeAggregatesList{}

		_, err = builder.LoadContext(ctx, &cvmIntervals)
		if err != nil {
			return nil, err
		}

		if len(intervals) == 0 {
			intervals = cvmIntervals
		} else {
			models.MergeAggregates(intervals.MergeList(), cvmIntervals.MergeList())
		}
	}

	// If no intervals were requested then the total aggregate is equal to the
	// first (and only) interval, and we're done
	if requestedIntervalCount == 0 {
		// This check should never fail if the SQL query is correct, but added for
		// robustness to prevent panics if the invariant does not hold.
		if len(intervals) > 0 {
			intervals[0].StartTime = params.ListParams.StartTime
			intervals[0].EndTime = params.ListParams.EndTime
			return &models.TxfeeAggregatesHistogram{
				TxfeeAggregates: intervals[0],
				StartTime:       params.ListParams.StartTime,
				EndTime:         params.ListParams.EndTime,
			}, nil
		}
		return &models.TxfeeAggregatesHistogram{
			StartTime: params.ListParams.StartTime,
			EndTime:   params.ListParams.EndTime,
		}, nil
	}

	// We need to return multiple intervals so build them now.
	// Intervals without any data will not return anything so we pad our results
	// with empty aggregates.
	//
	// We also add the start and end times of each interval to that interval
	aggs := &models.TxfeeAggregatesHistogram{IntervalSize: params.IntervalSize}

	var startTS int64
	timesForInterval := func(intervalIdx int) (time.Time, time.Time) {
		startTS = params.ListParams.StartTime.Unix() + (int64(intervalIdx) * intervalSeconds)
		return time.Unix(startTS, 0).UTC(),
			time.Unix(startTS+intervalSeconds-1, 0).UTC()
	}

	padTo := func(slice []models.TxfeeAggregates, to int) []models.TxfeeAggregates {
		for i := len(slice); i < to; i = len(slice) {
			slice = append(slice, models.TxfeeAggregates{IntervalID: i})
			slice[i].StartTime, slice[i].EndTime = timesForInterval(i)
		}
		return slice
	}

	// Collect the overall counts and pad the intervals to include empty intervals
	// which are not returned by the db
	aggs.TxfeeAggregates = models.TxfeeAggregates{StartTime: params.ListParams.StartTime, EndTime: params.ListParams.EndTime}
	var totalVolume uint64 = 0

	// Add each interval, but first pad up to that interval's index
	aggs.Intervals = make([]models.TxfeeAggregates, 0, requestedIntervalCount)
	for _, interval := range intervals {
		// Pad up to this interval's position
		aggs.Intervals = padTo(aggs.Intervals, interval.IntervalID)

		// Format this interval
		interval.StartTime, interval.EndTime = timesForInterval(interval.IntervalID)

		// Add to the overall aggregates counts
		totalVolume += interval.Txfee

		// Add to the list of intervals
		aggs.Intervals = append(aggs.Intervals, interval)
	}
	// Add total aggregated token amounts
	aggs.TxfeeAggregates.Txfee = totalVolume

	// Add any missing trailing intervals
	aggs.Intervals = padTo(aggs.Intervals, requestedIntervalCount)

	aggs.StartTime = params.ListParams.StartTime
	aggs.EndTime = params.ListParams.EndTime

	return aggs, nil
}

func (r *Reader) Aggregate(ctx context.Context, params *params.AggregateParams, conns *utils.Connections) (*models.AggregatesHistogram, error) {
	if !params.ListParams.Values.Has("cacheUpd") {
		// if the request is not coming from the caching mechanism then return the values of the cache and do NOT probe the database
		cache := models.AggregatesList{}
		var temp = models.Aggregates{}
		// based on the date interval we are going to retrieve the relevant part from our cache
		var keyDatePartValue = cfg.GetDatepartBasedOnDateParams(params.ListParams.StartTime, params.ListParams.EndTime)
		temp.TransactionCount = cfg.GetAggregateTransactionsMap()[params.ChainIDs[0]][keyDatePartValue]

		cache = append(cache, temp)
		cache[0].StartTime = params.ListParams.StartTime
		cache[0].EndTime = params.ListParams.EndTime
		return &models.AggregatesHistogram{
			Aggregates: cache[0],
			StartTime:  params.ListParams.StartTime,
			EndTime:    params.ListParams.EndTime,
		}, nil
	}
	// Validate params and set defaults if necessary
	if params.ListParams.StartTime.IsZero() {
		var err error
		params.ListParams.StartTime, err = r.getFirstTransactionTime(ctx, params.ChainIDs)
		if err != nil {
			return nil, err
		}
	}

	intervals := models.AggregatesList{}

	// Ensure the interval count requested isn't too large
	intervalSeconds := int64(params.IntervalSize.Seconds())
	requestedIntervalCount := 0
	if intervalSeconds != 0 {
		requestedIntervalCount = int(math.Ceil(params.ListParams.EndTime.Sub(params.ListParams.StartTime).Seconds() / params.IntervalSize.Seconds()))
		if requestedIntervalCount > MaxAggregateIntervalCount {
			return nil, ErrAggregateIntervalCountTooLarge
		}
		if requestedIntervalCount < 1 {
			requestedIntervalCount = 1
		}
	}

	// Split chains
	var avmChains, cvmChains []string
	if len(params.ChainIDs) == 0 {
		for id, chain := range r.sc.Chains {
			switch chain.VMType {
			case models.CVMName:
				cvmChains = append(cvmChains, id)
			default:
				avmChains = append(avmChains, id)
			}
		}
	} else {
		for _, id := range params.ChainIDs {
			chain, exist := r.sc.Chains[id]
			if exist {
				switch chain.VMType {
				case models.CVMName:
					cvmChains = append(cvmChains, id)
				default:
					avmChains = append(avmChains, id)
				}
			}
		}
	}

	var dbRunner *dbr.Session
	var err error

	if conns != nil {
		dbRunner = conns.DB().NewSessionForEventReceiver(conns.Stream().NewJob("get_transaction_aggregates_histogram"))
	} else {
		dbRunner, err = r.conns.DB().NewSession("get_transaction_aggregates_histogram", cfg.RequestTimeout)
		if err != nil {
			return nil, err
		}
	}

	var builder *dbr.SelectStmt

	if len(avmChains) > 0 {
		columns := []string{
			"COALESCE(SUM(avm_outputs.amount), 0) AS transaction_volume",
			"COUNT(DISTINCT(avm_outputs.transaction_id)) AS transaction_count",
			"COUNT(DISTINCT(avm_output_addresses.address)) AS address_count",
			"COUNT(DISTINCT(avm_outputs.asset_id)) AS asset_count",
			"COUNT(avm_outputs.id) AS output_count",
		}

		if requestedIntervalCount > 0 {
			columns = append(columns, fmt.Sprintf(
				"FLOOR((UNIX_TIMESTAMP(avm_outputs.created_at)-%d) / %d) AS interval_id",
				params.ListParams.StartTime.Unix(),
				intervalSeconds))
		}

		builder = dbRunner.
			Select(columns...).
			From("avm_outputs").
			LeftJoin("avm_output_addresses", "avm_output_addresses.output_id = avm_outputs.id").
			Where("avm_outputs.created_at >= ?", params.ListParams.StartTime).
			Where("avm_outputs.created_at < ?", params.ListParams.EndTime)

		if len(params.ChainIDs) != 0 {
			builder.Where("avm_outputs.chain_id IN ?", avmChains)
		}

		if params.AssetID != nil {
			builder.Where("avm_outputs.asset_id = ?", params.AssetID.String())
		}

		if requestedIntervalCount > 0 {
			builder.
				GroupBy("interval_id").
				OrderAsc("interval_id").
				Limit(uint64(requestedIntervalCount))
		}

		_, err = builder.LoadContext(ctx, &intervals)
		if err != nil {
			return nil, err
		}
	}

	if len(cvmChains) > 0 {
		columns := []string{
			"COALESCE(SUM(cvm_transactions_txdata.amount), 0) AS transaction_volume",
			"COUNT(cvm_transactions_txdata.hash) AS transaction_count",
			"COUNT(DISTINCT(cvm_transactions_txdata.id_from_addr)) AS address_count",
			"1 AS asset_count",
			"0 AS output_count",
		}

		if requestedIntervalCount > 0 {
			columns = append(columns, fmt.Sprintf(
				"FLOOR((UNIX_TIMESTAMP(cvm_transactions_txdata.created_at)-%d) / %d) AS interval_id",
				params.ListParams.StartTime.Unix(),
				intervalSeconds))
		}

		builder = dbRunner.
			Select(columns...).
			From("cvm_transactions_txdata")

		if len(params.ChainIDs) != 0 {
			builder.Join("cvm_blocks", "cvm_blocks.block = cvm_transactions_txdata.block").
				Where("cvm_blocks.chain_id IN ?", cvmChains)
		}

		builder.Where("cvm_transactions_txdata.created_at >= ?", params.ListParams.StartTime).
			Where("cvm_transactions_txdata.created_at < ?", params.ListParams.EndTime)

		if requestedIntervalCount > 0 {
			builder.
				GroupBy("interval_id").
				OrderAsc("interval_id").
				Limit(uint64(requestedIntervalCount))
		}

		cvmIntervals := models.AggregatesList{}

		_, err = builder.LoadContext(ctx, &cvmIntervals)
		if err != nil {
			return nil, err
		}

		if len(intervals) == 0 {
			intervals = cvmIntervals
		} else {
			models.MergeAggregates(intervals.MergeList(), cvmIntervals.MergeList())
		}
	}

	// If no intervals were requested then the total aggregate is equal to the
	// first (and only) interval, and we're done
	if requestedIntervalCount == 0 {
		// This check should never fail if the SQL query is correct, but added for
		// robustness to prevent panics if the invariant does not hold.
		if len(intervals) > 0 {
			intervals[0].StartTime = params.ListParams.StartTime
			intervals[0].EndTime = params.ListParams.EndTime
			return &models.AggregatesHistogram{
				Aggregates: intervals[0],
				StartTime:  params.ListParams.StartTime,
				EndTime:    params.ListParams.EndTime,
			}, nil
		}
		return &models.AggregatesHistogram{
			StartTime: params.ListParams.StartTime,
			EndTime:   params.ListParams.EndTime,
		}, nil
	}

	// We need to return multiple intervals so build them now.
	// Intervals without any data will not return anything so we pad our results
	// with empty aggregates.
	//
	// We also add the start and end times of each interval to that interval
	aggs := &models.AggregatesHistogram{IntervalSize: params.IntervalSize}

	var startTS int64
	timesForInterval := func(intervalIdx int) (time.Time, time.Time) {
		startTS = params.ListParams.StartTime.Unix() + (int64(intervalIdx) * intervalSeconds)
		return time.Unix(startTS, 0).UTC(),
			time.Unix(startTS+intervalSeconds-1, 0).UTC()
	}

	padTo := func(slice []models.Aggregates, to int) []models.Aggregates {
		for i := len(slice); i < to; i = len(slice) {
			slice = append(slice, models.Aggregates{IntervalID: i})
			slice[i].StartTime, slice[i].EndTime = timesForInterval(i)
		}
		return slice
	}

	// Collect the overall counts and pad the intervals to include empty intervals
	// which are not returned by the db
	aggs.Aggregates = models.Aggregates{StartTime: params.ListParams.StartTime, EndTime: params.ListParams.EndTime}
	var (
		bigIntFromStringOK bool
		totalVolume        = big.NewInt(0)
		intervalVolume     = big.NewInt(0)
	)

	// Add each interval, but first pad up to that interval's index
	aggs.Intervals = make([]models.Aggregates, 0, requestedIntervalCount)
	for _, interval := range intervals {
		// Pad up to this interval's position
		aggs.Intervals = padTo(aggs.Intervals, interval.IntervalID)

		// Format this interval
		interval.StartTime, interval.EndTime = timesForInterval(interval.IntervalID)

		// Parse volume into a big.Int
		_, bigIntFromStringOK = intervalVolume.SetString(string(interval.TransactionVolume), 10)
		if !bigIntFromStringOK {
			return nil, ErrFailedToParseStringAsBigInt
		}

		// Add to the overall aggregates counts
		totalVolume.Add(totalVolume, intervalVolume)
		aggs.Aggregates.TransactionCount += interval.TransactionCount
		aggs.Aggregates.OutputCount += interval.OutputCount
		aggs.Aggregates.AddressCount += interval.AddressCount
		aggs.Aggregates.AssetCount += interval.AssetCount

		// Add to the list of intervals
		aggs.Intervals = append(aggs.Intervals, interval)
	}
	// Add total aggregated token amounts
	aggs.Aggregates.TransactionVolume = models.TokenAmount(totalVolume.String())

	// Add any missing trailing intervals
	aggs.Intervals = padTo(aggs.Intervals, requestedIntervalCount)

	aggs.StartTime = params.ListParams.StartTime
	aggs.EndTime = params.ListParams.EndTime

	return aggs, nil
}

func (r *Reader) ListAddresses(ctx context.Context, p *params.ListAddressesParams) (*models.AddressList, error) {
	dbRunner, err := r.conns.DB().NewSession("list_addresses", cfg.RequestTimeout)
	if err != nil {
		return nil, err
	}

	var rows []*struct {
		ChainID models.StringID `json:"chainID"`
		Address models.Address  `json:"address"`
		models.AssetInfo
		PublicKey []byte `json:"publicKey"`
	}

	var ua *dbr.SelectStmt
	var baseq *dbr.SelectStmt

	if r.sc.IsAccumulateBalanceReader {
		ua = dbRunner.Select("avm_outputs.chain_id", "avm_output_addresses.address").
			Distinct().
			From("avm_outputs").
			LeftJoin("avm_output_addresses", "avm_outputs.id = avm_output_addresses.output_id").
			OrderAsc("avm_outputs.chain_id").
			OrderAsc("avm_output_addresses.address")

		baseq = p.Apply(dbRunner.
			Select(
				"accumulate_balances_received.chain_id",
				"accumulate_balances_received.address",
				"accumulate_balances_received.asset_id",
				"accumulate_balances_transactions.transaction_count",
				"accumulate_balances_received.total_amount as total_received",
				"case when accumulate_balances_sent.total_amount is null then 0 else accumulate_balances_sent.total_amount end as total_sent",
				"accumulate_balances_received.total_amount - case when accumulate_balances_sent.total_amount is null then 0 else accumulate_balances_sent.total_amount end as balance",
				"accumulate_balances_received.utxo_count - case when accumulate_balances_sent.utxo_count is null then 0 else accumulate_balances_sent.utxo_count end as utxo_count",
			).
			From("accumulate_balances_received").
			LeftJoin("accumulate_balances_sent", "accumulate_balances_received.id = accumulate_balances_sent.id").
			LeftJoin("accumulate_balances_transactions", "accumulate_balances_received.id = accumulate_balances_transactions.id").
			OrderAsc("accumulate_balances_received.chain_id").
			OrderAsc("accumulate_balances_received.address").
			OrderAsc("accumulate_balances_received.asset_id"), true)
	} else {
		ua = dbRunner.Select("avm_outputs.chain_id", "avm_output_addresses.address").
			Distinct().
			From("avm_outputs").
			LeftJoin("avm_output_addresses", "avm_outputs.id = avm_output_addresses.output_id").
			OrderAsc("avm_outputs.chain_id").
			OrderAsc("avm_output_addresses.address")

		baseq = dbRunner.
			Select(
				"avm_outputs.chain_id",
				"avm_output_addresses.address",
				"avm_outputs.asset_id",
				"COUNT(DISTINCT(avm_outputs.transaction_id)) AS transaction_count",
				"COALESCE(SUM(avm_outputs.amount), 0) AS total_received",
				"COALESCE(SUM(CASE WHEN avm_outputs_redeeming.redeeming_transaction_id IS NOT NULL THEN avm_outputs.amount ELSE 0 END), 0) AS total_sent",
				"COALESCE(SUM(CASE WHEN avm_outputs_redeeming.redeeming_transaction_id IS NULL THEN avm_outputs.amount ELSE 0 END), 0) AS balance",
				"COALESCE(SUM(CASE WHEN avm_outputs_redeeming.redeeming_transaction_id IS NULL THEN 1 ELSE 0 END), 0) AS utxo_count",
			).
			From("avm_outputs").
			LeftJoin("avm_output_addresses", "avm_output_addresses.output_id = avm_outputs.id").
			LeftJoin("avm_outputs_redeeming", "avm_outputs.id = avm_outputs_redeeming.id").
			Where("avm_output_addresses.address in ?", dbRunner.Select(
				"avm_outputs_ua.address",
			).From(p.Apply(ua, false).As("avm_outputs_ua"))).
			GroupBy("avm_outputs.chain_id", "avm_output_addresses.address", "avm_outputs.asset_id").
			OrderAsc("avm_outputs.chain_id").
			OrderAsc("avm_output_addresses.address").
			OrderAsc("avm_outputs.asset_id")

		if len(p.ChainIDs) != 0 {
			baseq.Where("avm_outputs.chain_id IN ?", p.ChainIDs)
		}
	}

	builder := dbRunner.Select(
		"avm_outputs_j.chain_id",
		"avm_outputs_j.address",
		"avm_outputs_j.asset_id",
		"avm_outputs_j.transaction_count",
		"avm_outputs_j.total_received",
		"avm_outputs_j.total_sent",
		"avm_outputs_j.balance",
		"avm_outputs_j.utxo_count",
		"addresses.public_key",
	).From(baseq.As("avm_outputs_j")).
		LeftJoin("addresses", "addresses.address = avm_outputs_j.address")

	_, err = builder.
		LoadContext(ctx, &rows)
	if err != nil {
		return nil, err
	}

	addresses := make([]*models.AddressInfo, 0, len(rows))

	addrsByID := make(map[string]*models.AddressInfo)

	for _, row := range rows {
		k := fmt.Sprintf("%s:%s", row.ChainID, row.Address)
		addr, ok := addrsByID[k]
		if !ok {
			addr = &models.AddressInfo{
				ChainID:   row.ChainID,
				Address:   row.Address,
				PublicKey: row.PublicKey,
				Assets:    make(map[models.StringID]models.AssetInfo),
			}
			addrsByID[k] = addr
		}
		addr.Assets[row.AssetID] = row.AssetInfo
	}
	for _, addr := range addrsByID {
		addresses = append(addresses, addr)
	}

	var count *uint64
	if !p.ListParams.DisableCounting {
		count = uint64Ptr(uint64(p.ListParams.Offset) + uint64(len(addresses)))
		if len(addresses) >= p.ListParams.Limit {
			p.ListParams = params.ListParams{}
			sqc := p.Apply(ua, true)
			buildercnt := dbRunner.Select(
				"count(*)",
			).From(sqc.As("avm_outputs_j"))
			err = buildercnt.
				LoadOneContext(ctx, &count)
			if err != nil {
				return nil, err
			}
		}
	}

	return &models.AddressList{ListMetadata: models.ListMetadata{Count: count}, Addresses: addresses}, nil
}

func (r *Reader) ListOutputs(ctx context.Context, p *params.ListOutputsParams) (*models.OutputList, error) {
	dbRunner, err := r.conns.DB().NewSession("list_transaction_outputs", cfg.RequestTimeout)
	if err != nil {
		return nil, err
	}

	var outputs []*models.Output
	_, err = p.Apply(dbRunner.
		Select(outputSelectColumns...).
		From("avm_outputs").
		LeftJoin("avm_outputs_redeeming", "avm_outputs.id = avm_outputs_redeeming.id")).
		LoadContext(ctx, &outputs)
	if err != nil {
		return nil, err
	}

	if len(outputs) < 1 {
		return &models.OutputList{Outputs: outputs}, nil
	}

	outputIDs := make([]models.StringID, len(outputs))
	outputMap := make(map[models.StringID]*models.Output, len(outputs))
	for i, output := range outputs {
		outputIDs[i] = output.ID
		outputMap[output.ID] = output
	}

	var addresses []*models.OutputAddress
	_, err = dbRunner.
		Select(
			"avm_output_addresses.output_id",
			"avm_output_addresses.address",
			"avm_output_addresses.redeeming_signature AS signature",
			"avm_output_addresses.created_at",
		).
		From("avm_output_addresses").
		Where("avm_output_addresses.output_id IN ?", outputIDs).
		LoadContext(ctx, &addresses)
	if err != nil {
		return nil, err
	}

	for _, address := range addresses {
		output := outputMap[address.OutputID]
		if output == nil {
			continue
		}
		output.Addresses = append(output.Addresses, address.Address)
	}

	var count *uint64
	if !p.ListParams.DisableCounting {
		count = uint64Ptr(uint64(p.ListParams.Offset) + uint64(len(outputs)))
		if len(outputs) >= p.ListParams.Limit {
			p.ListParams = params.ListParams{}
			err = p.Apply(dbRunner.
				Select("COUNT(avm_outputs.id)").
				From("avm_outputs").
				LeftJoin("avm_outputs_redeeming", "avm_outputs.id = avm_outputs_redeeming.id")).
				LoadOneContext(ctx, &count)
			if err != nil {
				return nil, err
			}
		}
	}

	return &models.OutputList{ListMetadata: models.ListMetadata{Count: count}, Outputs: outputs}, err
}

func (r *Reader) GetTransaction(ctx context.Context, id ids.ID, avaxAssetID ids.ID) (*models.Transaction, error) {
	txList, err := r.ListTransactions(ctx, &params.ListTransactionsParams{
		ListParams: params.ListParams{ID: &id, DisableCounting: true},
	}, avaxAssetID)
	if err != nil {
		return nil, err
	}
	if len(txList.Transactions) > 0 {
		return txList.Transactions[0], nil
	}
	return nil, nil
}

func (r *Reader) GetAddress(ctx context.Context, p *params.ListAddressesParams) (*models.AddressInfo, error) {
	addressList, err := r.ListAddresses(ctx, p)
	if err != nil {
		return nil, err
	}
	if len(addressList.Addresses) > 1 {
		collated := make(map[string]*models.AddressInfo)
		for _, a := range addressList.Addresses {
			key := string(a.Address)
			if addressInfo, ok := collated[key]; ok {
				if addressInfo.Assets == nil {
					addressInfo.Assets = make(map[models.StringID]models.AssetInfo)
				}
				collated[key].ChainID = ""
				collated[key].Assets = addAssetInfoMap(addressInfo.Assets, a.Assets)
			} else {
				collated[key] = a
			}
		}
		addressList.Addresses = []*models.AddressInfo{}
		for _, v := range collated {
			addressList.Addresses = append(addressList.Addresses, v)
		}
	}
	if len(addressList.Addresses) > 0 {
		return addressList.Addresses[0], nil
	}
	return nil, err
}

func (r *Reader) GetOutput(ctx context.Context, id ids.ID) (*models.Output, error) {
	outputList, err := r.ListOutputs(ctx,
		&params.ListOutputsParams{
			ListParams: params.ListParams{ID: &id, DisableCounting: true},
		})
	if err != nil {
		return nil, err
	}
	if len(outputList.Outputs) > 0 {
		return outputList.Outputs[0], nil
	}
	return nil, err
}

func (r *Reader) AddressChains(ctx context.Context, p *params.AddressChainsParams) (*models.AddressChains, error) {
	dbRunner, err := r.conns.DB().NewSession("addressChains", cfg.RequestTimeout)
	if err != nil {
		return nil, err
	}

	var addressChains []*models.AddressChainInfo

	// if there are no addresses specified don't query.
	if len(p.Addresses) == 0 {
		return &models.AddressChains{}, nil
	}

	_, err = p.Apply(dbRunner.
		Select("address", "chain_id", "created_at").
		From("address_chain")).
		LoadContext(ctx, &addressChains)
	if err != nil {
		return nil, err
	}

	resp := models.AddressChains{}
	resp.AddressChains = make(map[string][]models.StringID)
	for _, addressChain := range addressChains {
		addr, err := addressChain.Address.MarshalString()
		if err != nil {
			return nil, err
		}
		addrAsStr := string(addr)
		if _, ok := resp.AddressChains[addrAsStr]; !ok {
			resp.AddressChains[addrAsStr] = make([]models.StringID, 0, 2)
		}
		resp.AddressChains[addrAsStr] = append(resp.AddressChains[addrAsStr], addressChain.ChainID)
	}

	return &resp, nil
}

func (r *Reader) getFirstTransactionTime(ctx context.Context, chainIDs []string) (time.Time, error) {
	dbRunner, err := r.conns.DB().NewSession("get_first_transaction_time", cfg.RequestTimeout)
	if err != nil {
		return time.Time{}, err
	}

	var ts float64
	builder := dbRunner.
		Select("COALESCE(UNIX_TIMESTAMP(MIN(created_at)), 0)").
		From("avm_transactions")

	if len(chainIDs) > 0 {
		builder.Where("avm_transactions.chain_id IN ?", chainIDs)
	}

	err = builder.LoadOneContext(ctx, &ts)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(int64(math.Floor(ts)), 0).UTC(), nil
}

func (r *Reader) searchByID(ctx context.Context, id ids.ID, avaxAssetID ids.ID) (*models.SearchResults, error) {
	dbRunner, err := r.conns.DB().NewSession("search_by_id", cfg.RequestTimeout)
	if err != nil {
		return nil, err
	}

	var txs []*models.Transaction
	builder := transactionQuery(dbRunner).
		Where("avm_transactions.id = ?", id.String()).Limit(1)
	if _, err := builder.LoadContext(ctx, &txs); err != nil {
		return nil, err
	}

	if len(txs) > 0 {
		return collateSearchResults(nil, nil, txs, nil, nil, nil)
	}

	if false {
		if txs, err := r.ListTransactions(ctx, &params.ListTransactionsParams{
			ListParams: params.ListParams{
				DisableCounting: true,
				ID:              &id,
			},
		}, avaxAssetID); err != nil {
			return nil, err
		} else if len(txs.Transactions) > 0 {
			return collateSearchResults(nil, nil, txs.Transactions, nil, nil, nil)
		}
	}

	return &models.SearchResults{}, nil
}

func (r *Reader) searchByShortID(ctx context.Context, id ids.ShortID) (*models.SearchResults, error) {
	listParams := params.ListParams{DisableCounting: true}

	if addrs, err := r.ListAddresses(ctx, &params.ListAddressesParams{ListParams: listParams, Address: &id}); err != nil {
		return nil, err
	} else if len(addrs.Addresses) > 0 {
		return collateSearchResults(nil, addrs.Addresses, nil, nil, nil, nil)
	}

	return &models.SearchResults{}, nil
}

func (r *Reader) searchCBlockHeight(ctx context.Context, height uint64) ([]models.CResult, error) {
	dbRunner, err := r.conns.DB().NewSession("search_cblock_height", cfg.RequestTimeout)
	if err != nil {
		return nil, err
	}

	results := []models.CResult{}

	if _, err := dbRunner.Select("block as number, hash").
		From(db.TableCvmBlocks).
		Where("block=?", height).
		LoadContext(ctx, &results); err != nil {
		return nil, err
	}
	return results, nil
}

func (r *Reader) searchCBlockHash(ctx context.Context, hash string) ([]models.CResult, error) {
	dbRunner, err := r.conns.DB().NewSession("search_cblock_hash", cfg.RequestTimeout)
	if err != nil {
		return nil, err
	}

	results := []models.CResult{}

	if _, err := dbRunner.Select("block as number, hash").
		From(db.TableCvmBlocks).
		Where("hash=?", hash).
		LoadContext(ctx, &results); err != nil {
		return nil, err
	}
	return results, nil
}

func (r *Reader) searchCTransHash(ctx context.Context, hash string) ([]models.CResult, error) {
	dbRunner, err := r.conns.DB().NewSession("search_ctrans_hash", cfg.RequestTimeout)
	if err != nil {
		return nil, err
	}

	results := []models.CResult{}

	if _, err := dbRunner.Select("hash").
		From(db.TableCvmTransactionsTxdata).
		Where("hash=?", hash).
		LoadContext(ctx, &results); err != nil {
		return nil, err
	}
	return results, nil
}

func (r *Reader) searchCAddress(ctx context.Context, address string) ([]models.CResult, error) {
	dbRunner, err := r.conns.DB().NewSession("search_cblock_hash", cfg.RequestTimeout)
	if err != nil {
		return nil, err
	}

	results := []models.CResult{}

	if _, err := dbRunner.Select("creation_tx IS NOT NULL AS number, address AS hash").
		From(db.TableCvmAccounts).
		Where("address=?", strings.ToLower(address)).
		LoadContext(ctx, &results); err != nil {
		return nil, err
	}
	return results, nil
}

func collateSearchResults(
	assets []*models.Asset,
	addresses []*models.AddressInfo,
	transactions []*models.Transaction,
	cblocks []models.CResult,
	ctrans []models.CResult,
	caddr []models.CResult,
) (*models.SearchResults, error) {
	// Build overall SearchResults object from our pieces
	returnedResultCount := len(assets) + len(addresses) + len(transactions) + len(cblocks) + len(ctrans) + len(caddr)
	if returnedResultCount > params.PaginationMaxLimit {
		returnedResultCount = params.PaginationMaxLimit
	}

	collatedResults := &models.SearchResults{
		Count: uint64(returnedResultCount),

		// Create a container for our combined results
		Results: make([]models.SearchResult, 0, returnedResultCount),
	}

	appendSR := func(resultType models.SearchResultType, data interface{}) bool {
		if len(collatedResults.Results) > returnedResultCount {
			return false
		}
		collatedResults.Results = append(collatedResults.Results, models.SearchResult{
			SearchResultType: resultType,
			Data:             data,
		})
		return true
	}

	// Add each result to the list
	for _, result := range addresses {
		if !appendSR(models.ResultTypeAddress, result) {
			break
		}
	}
	for _, result := range transactions {
		if !appendSR(models.ResultTypeTransaction, result) {
			break
		}
	}
	for _, result := range assets {
		if !appendSR(models.ResultTypeAsset, result) {
			break
		}
	}
	for _, result := range cblocks {
		if !appendSR(models.ResultTypeCBlock, result) {
			break
		}
	}
	for _, result := range ctrans {
		if !appendSR(models.ResultTypeCTrans, result) {
			break
		}
	}
	for _, result := range caddr {
		if !appendSR(models.ResultTypeCAddress, result) {
			break
		}
	}
	return collatedResults, nil
}

func transactionQuery(dbRunner *dbr.Session) *dbr.SelectStmt {
	return dbRunner.
		Select(
			"avm_transactions.id",
			"avm_transactions.chain_id",
			"avm_transactions.type",
			"avm_transactions.memo",
			"avm_transactions.created_at",
			"avm_transactions.txfee",
			"avm_transactions.genesis",
			"case when transactions_epoch.epoch is null then 0 else transactions_epoch.epoch end as epoch",
			"case when transactions_epoch.vertex_id is null then '' else transactions_epoch.vertex_id end as vertex_id",
			"case when transactions_validator.node_id is null then '' else transactions_validator.node_id end as validator_node_id",
			"case when transactions_validator.start is null then 0 else transactions_validator.start end as validator_start",
			"case when transactions_validator.end is null then 0 else transactions_validator.end end as validator_end",
			"case when transactions_block.tx_block_id is null then '' else transactions_block.tx_block_id end as tx_block_id",
		).
		From("avm_transactions").
		LeftJoin("transactions_epoch", "avm_transactions.id = transactions_epoch.id").
		LeftJoin("transactions_validator", "avm_transactions.id = transactions_validator.id").
		LeftJoin("transactions_block", "avm_transactions.id = transactions_block.id")
}

func (r *Reader) chainWriter(chainID string) (services.Consumer, error) {
	r.avmLock.RLock()
	w, ok := r.chainConsumers[chainID]
	r.avmLock.RUnlock()
	if ok {
		return w, nil
	}
	return nil, fmt.Errorf("unimplemented")
}

func (r *Reader) ATxDATA(ctx context.Context, p *params.TxDataParam) ([]byte, error) {
	dbRunner, err := r.conns.DB().NewSession("atx_data", cfg.RequestTimeout)
	if err != nil {
		return nil, err
	}

	type Row struct {
		Serialization []byte
		ChainID       string
	}
	rows := []Row{}

	_, err = dbRunner.
		Select("canonical_serialization as serialization", "chain_id").
		From("avm_transactions").
		Where("id=?", p.ID).
		LoadContext(ctx, &rows)
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		return []byte(""), nil
	}

	row := rows[0]

	var c services.Consumer
	c, err = r.chainWriter(row.ChainID)
	if err != nil {
		return nil, err
	}
	j, err := c.ParseJSON(row.Serialization, nil)
	if err != nil {
		return nil, err
	}
	return j, nil
}

func (r *Reader) PTxDATA(ctx context.Context, p *params.TxDataParam) ([]byte, error) {
	dbRunner, err := r.conns.DB().NewSession("ptx_data", cfg.RequestTimeout)
	if err != nil {
		return nil, err
	}

	type Row struct {
		ID            string
		Serialization []byte
		ChainID       string
		Proposer      string
		ProposerTime  *time.Time
	}
	rows := []Row{}

	idInt, ok := big.NewInt(0).SetString(p.ID, 10)
	if idInt != nil && ok {
		_, err = dbRunner.
			Select("id", "serialization", "chain_id", "proposer", "proposer_time").
			From(db.TablePvmBlocks).
			Where("height="+idInt.String()).
			LoadContext(ctx, &rows)
		if err != nil {
			return nil, err
		}
	} else {
		_, err = dbRunner.
			Select("id", "serialization", "chain_id", "proposer", "proposer_time").
			From(db.TablePvmBlocks).
			Where("id=?", p.ID).
			LoadContext(ctx, &rows)
		if err != nil {
			return nil, err
		}
	}
	if len(rows) == 0 {
		return []byte(""), nil
	}

	row := rows[0]

	var c services.Consumer
	c, err = r.chainWriter(row.ChainID)
	if err != nil {
		return nil, err
	}
	j, err := c.ParseJSON(row.Serialization,
		&models.BlockProposal{Proposer: row.Proposer, TimeStamp: row.ProposerTime})
	if err != nil {
		return nil, err
	}
	return j, nil
}

func (r *Reader) CTxDATA(ctx context.Context, p *params.TxDataParam) ([]byte, error) {
	dbRunner, err := r.conns.DB().NewSession("ctx_data", cfg.RequestTimeout)
	if err != nil {
		return nil, err
	}

	idInt, ok := big.NewInt(0).SetString(p.ID, 10)
	if !ok {
		err = dbRunner.
			Select("MAX(block)").
			From(db.TableCvmBlocks).
			Limit(1).
			LoadOneContext(ctx, &idInt)
		if err != nil {
			return nil, err
		}
	}

	// copy of the Block object for export to json
	type BlockExport struct {
		Hash         string                     `json:"hash"`
		Header       corethType.Header          `json:"header"`
		Transactions []*models.CTransactionData `json:"transactions"`
	}

	// Load the block header
	cvmBlock := &db.CvmBlocks{Block: idInt.String()}
	cvmBlock, err = r.sc.Persist.QueryCvmBlock(ctx, dbRunner, cvmBlock)
	if err != nil {
		return nil, err
	}

	block := BlockExport{Hash: cvmBlock.Hash}

	err = block.Header.UnmarshalJSON(cvmBlock.Serialization)
	if err != nil {
		return nil, err
	}

	// Load Transactions and signatures
	cTransactionList, err := r.ListCTransactions(ctx, &params.ListCTransactionsParams{BlockStart: idInt, BlockEnd: idInt})
	if err != nil {
		return nil, err
	}
	block.Transactions = cTransactionList.Transactions

	return json.Marshal(block)
}

func uint64Ptr(u64 uint64) *uint64 {
	return &u64
}
