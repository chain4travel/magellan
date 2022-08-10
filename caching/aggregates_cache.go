package caching

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"net/url"
	"time"

	"github.com/chain4travel/magellan/cfg"
	"github.com/chain4travel/magellan/models"
	"github.com/chain4travel/magellan/services/indexes/params"
	"github.com/chain4travel/magellan/utils"
	"github.com/gocraft/dbr/v2"
)

const (
	MaxAggregateIntervalCount = 20000
)

var (
	ErrAggregateIntervalCountTooLarge = errors.New("requesting too many intervals")
	ErrFailedToParseStringAsBigInt    = errors.New("failed to parse string to big.Int")
)

type AggregatesCache interface {
	GetAggregateTransactionsMap() map[string]map[string]uint64
	GetAggregateFeesMap() map[string]map[string]uint64
	InitCacheStorage(cfg.Chains)
	GetAggregatesFeesAndUpdate(map[string]cfg.Chain, *utils.Connections, string, time.Time, time.Time, string) error
	GetAggregatesAndUpdate(map[string]cfg.Chain, *utils.Connections, string, time.Time, time.Time, string) error
}

type aggregatesCache struct {
	aggregateTransactionsMap map[string]map[string]uint64
	aggregateFeesMap         map[string]map[string]uint64
}

func NewAggregatesCache() AggregatesCache {
	return &aggregatesCache{
		aggregateTransactionsMap: make(map[string]map[string]uint64),
		aggregateFeesMap:         make(map[string]map[string]uint64),
	}
}

func (ac *aggregatesCache) GetAggregateTransactionsMap() map[string]map[string]uint64 {
	return ac.aggregateTransactionsMap
}

func (ac *aggregatesCache) GetAggregateFeesMap() map[string]map[string]uint64 {
	return ac.aggregateFeesMap
}

func (ac *aggregatesCache) InitCacheStorage(chains cfg.Chains) {
	aggregateTransMap := ac.aggregateTransactionsMap
	aggregateFeesMap := ac.aggregateFeesMap
	for id := range chains {
		aggregateTransMap[id] = map[string]uint64{}
		aggregateFeesMap[id] = map[string]uint64{}

		// we initialize the value for all 3 chains
		aggregateTransMap[id]["day"] = 0
		aggregateTransMap[id]["week"] = 0
		aggregateTransMap[id]["month"] = 0

		// we initialize the value for all 3 chains also here
		aggregateFeesMap[id]["day"] = 0
		aggregateFeesMap[id]["week"] = 0
		aggregateFeesMap[id]["month"] = 0
	}
}

//gocyclo:ignore
func (ac *aggregatesCache) GetAggregatesAndUpdate(chains map[string]cfg.Chain, conns *utils.Connections, chainid string, startTime time.Time, endTime time.Time, rangeKeyType string) error {
	chainIds := []string{chainid}
	// Validate params and set defaults if necessary
	if startTime.IsZero() {
		var err error
		startTime, err = getFirstTransactionTime(conns, chainIds)
		if err != nil {
			return err
		}
	}

	intervals := models.AggregatesList{}
	urlv := url.Values{}
	assetID, err := params.GetQueryID(urlv, params.KeyAssetID)
	if err != nil {
		return err
	}
	intervalSize, err := params.GetQueryInterval(urlv, params.KeyIntervalSize)
	if err != nil {
		return err
	}

	// Ensure the interval count requested isn't too large
	intervalSeconds := int64(intervalSize.Seconds())
	requestedIntervalCount := 0
	if intervalSeconds != 0 {
		requestedIntervalCount = int(math.Ceil(endTime.Sub(startTime).Seconds() / intervalSize.Seconds()))
		if requestedIntervalCount > MaxAggregateIntervalCount {
			return ErrAggregateIntervalCountTooLarge
		}
		if requestedIntervalCount < 1 {
			requestedIntervalCount = 1
		}
	}

	// Split chains
	var avmChains, cvmChains []string
	if len(chainIds) == 0 {
		for id, chain := range chains {
			switch chain.VMType {
			case models.CVMName:
				cvmChains = append(cvmChains, id)
			default:
				avmChains = append(avmChains, id)
			}
		}
	} else {
		for _, id := range chainIds {
			chain, exist := chains[id]
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

	if conns != nil {
		dbRunner = conns.DB().NewSessionForEventReceiver(conns.Stream().NewJob("get_transaction_aggregates_histogram"))
	} else {
		dbRunner, err = conns.DB().NewSession("get_transaction_aggregates_histogram", cfg.RequestTimeout)
		if err != nil {
			return err
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
				startTime.Unix(),
				intervalSeconds))
		}

		builder = dbRunner.
			Select(columns...).
			From("avm_outputs").
			LeftJoin("avm_output_addresses", "avm_output_addresses.output_id = avm_outputs.id").
			Where("avm_outputs.created_at >= ?", startTime).
			Where("avm_outputs.created_at < ?", endTime)

		if len(chainIds) != 0 {
			builder.Where("avm_outputs.chain_id IN ?", avmChains)
		}

		if assetID != nil {
			builder.Where("avm_outputs.asset_id = ?", assetID.String())
		}

		if requestedIntervalCount > 0 {
			builder.
				GroupBy("interval_id").
				OrderAsc("interval_id").
				Limit(uint64(requestedIntervalCount))
		}

		_, err = builder.Load(&intervals)
		if err != nil {
			return err
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
				startTime.Unix(),
				intervalSeconds))
		}

		builder = dbRunner.
			Select(columns...).
			From("cvm_transactions_txdata")

		if len(chainIds) != 0 {
			builder.Join("cvm_blocks", "cvm_blocks.block = cvm_transactions_txdata.block").
				Where("cvm_blocks.chain_id IN ?", cvmChains)
		}

		builder.Where("cvm_transactions_txdata.created_at >= ?", startTime).
			Where("cvm_transactions_txdata.created_at < ?", endTime)

		if requestedIntervalCount > 0 {
			builder.
				GroupBy("interval_id").
				OrderAsc("interval_id").
				Limit(uint64(requestedIntervalCount))
		}

		cvmIntervals := models.AggregatesList{}

		_, err = builder.Load(&cvmIntervals)
		if err != nil {
			return err
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
			intervals[0].StartTime = startTime
			intervals[0].EndTime = endTime
			aggs := &models.AggregatesHistogram{
				Aggregates: intervals[0],
				StartTime:  startTime,
				EndTime:    endTime,
			}
			ac.aggregateTransactionsMap[chainid][rangeKeyType] = aggs.Aggregates.TransactionCount
			return nil
		}
		aggs := &models.AggregatesHistogram{
			StartTime: startTime,
			EndTime:   endTime,
		}
		ac.aggregateTransactionsMap[chainid][rangeKeyType] = aggs.Aggregates.TransactionCount
		return nil
	}

	// We need to return multiple intervals so build them now.
	// Intervals without any data will not return anything so we pad our results
	// with empty aggregates.
	//
	// We also add the start and end times of each interval to that interval
	aggs := &models.AggregatesHistogram{IntervalSize: intervalSize}

	var startTS int64
	timesForInterval := func(intervalIdx int) (time.Time, time.Time) {
		startTS = startTime.Unix() + (int64(intervalIdx) * intervalSeconds)
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
	aggs.Aggregates = models.Aggregates{StartTime: startTime, EndTime: endTime}
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
			return ErrFailedToParseStringAsBigInt
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

	aggs.StartTime = startTime
	aggs.EndTime = endTime
	ac.aggregateTransactionsMap[chainid][rangeKeyType] = aggs.Aggregates.TransactionCount
	return nil
}

//gocyclo:ignore
func (ac *aggregatesCache) GetAggregatesFeesAndUpdate(chains map[string]cfg.Chain, conns *utils.Connections, chainid string, startTime time.Time, endTime time.Time, rangeKeyType string) error {
	chainIds := []string{chainid}
	// Validate params and set defaults if necessary
	if startTime.IsZero() {
		var err error
		startTime, err = getFirstTransactionTime(conns, chainIds)
		if err != nil {
			return err
		}
	}

	intervals := models.TxfeeAggregatesList{}
	urlv := url.Values{}
	intervalSize, err := params.GetQueryInterval(urlv, params.KeyIntervalSize)
	if err != nil {
		return err
	}

	// Ensure the interval count requested isn't too large
	intervalSeconds := int64(intervalSize.Seconds())
	requestedIntervalCount := 0
	if intervalSeconds != 0 {
		requestedIntervalCount = int(math.Ceil(endTime.Sub(startTime).Seconds() / intervalSize.Seconds()))
		if requestedIntervalCount > MaxAggregateIntervalCount {
			return ErrAggregateIntervalCountTooLarge
		}
		if requestedIntervalCount < 1 {
			requestedIntervalCount = 1
		}
	}

	// Split chains
	var avmChains, cvmChains []string
	if len(chainIds) == 0 {
		for id, chain := range chains {
			switch chain.VMType {
			case models.CVMName:
				cvmChains = append(cvmChains, id)
			default:
				avmChains = append(avmChains, id)
			}
		}
	} else {
		for _, id := range chainIds {
			chain, exist := chains[id]
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
	dbRunner, err := conns.DB().NewSession("get_txfee_aggregates_histogram", cfg.RequestTimeout)
	if err != nil {
		return err
	}

	var builder *dbr.SelectStmt

	if len(avmChains) > 0 {
		columns := []string{
			"CAST(COALESCE(SUM(avm_transactions.txfee), 0) AS UNSIGNED) AS txfee",
		}

		if requestedIntervalCount > 0 {
			columns = append(columns, fmt.Sprintf(
				"FLOOR((UNIX_TIMESTAMP(avm_transactions.created_at)-%d) / %d) AS interval_id",
				startTime.Unix(),
				intervalSeconds))
		}

		builder = dbRunner.
			Select(columns...).
			From("avm_transactions").
			Where("avm_transactions.created_at >= ?", startTime).
			Where("avm_transactions.created_at < ?", endTime)

		if requestedIntervalCount > 0 {
			builder.
				GroupBy("interval_id").
				OrderAsc("interval_id").
				Limit(uint64(requestedIntervalCount))
		}

		if len(chainIds) != 0 {
			builder.Where("avm_transactions.chain_id IN ?", chainIds)
		}

		_, err = builder.Load(&intervals)
		if err != nil {
			return err
		}
	}

	if len(cvmChains) > 0 {
		columns := []string{
			"CAST(COALESCE(SUM((cvm_transactions_txdata.gas_price / 1000000000) * cvm_transactions_txdata.gas_used), 0) AS UNSIGNED) AS txfee",
		}

		if requestedIntervalCount > 0 {
			columns = append(columns, fmt.Sprintf(
				"FLOOR((UNIX_TIMESTAMP(cvm_transactions_txdata.created_at)-%d) / %d) AS interval_id",
				startTime.Unix(),
				intervalSeconds))
		}

		builder = dbRunner.
			Select(columns...).
			From("cvm_transactions_txdata").
			Where("cvm_transactions_txdata.created_at >= ?", startTime).
			Where("cvm_transactions_txdata.created_at < ?", endTime)

		if requestedIntervalCount > 0 {
			builder.
				GroupBy("interval_id").
				OrderAsc("interval_id").
				Limit(uint64(requestedIntervalCount))
		}

		if len(chainIds) != 0 {
			builder.
				Join("cvm_blocks", "cvm_blocks.block = cvm_transactions_txdata.block").
				Where("cvm_blocks.chain_id IN ?", cvmChains)
		}

		cvmIntervals := models.TxfeeAggregatesList{}

		_, err = builder.Load(&cvmIntervals)
		if err != nil {
			return err
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
			intervals[0].StartTime = startTime
			intervals[0].EndTime = endTime
			aggs := &models.TxfeeAggregatesHistogram{
				TxfeeAggregates: intervals[0],
				StartTime:       startTime,
				EndTime:         endTime,
			}
			ac.aggregateFeesMap[chainid][rangeKeyType] = aggs.TxfeeAggregates.Txfee
			return nil
		}
		aggs := &models.TxfeeAggregatesHistogram{
			StartTime: startTime,
			EndTime:   endTime,
		}
		ac.aggregateFeesMap[chainid][rangeKeyType] = aggs.TxfeeAggregates.Txfee
		return nil
	}

	// We need to return multiple intervals so build them now.
	// Intervals without any data will not return anything so we pad our results
	// with empty aggregates.
	//
	// We also add the start and end times of each interval to that interval
	aggs := &models.TxfeeAggregatesHistogram{IntervalSize: intervalSize}

	var startTS int64
	timesForInterval := func(intervalIdx int) (time.Time, time.Time) {
		startTS = startTime.Unix() + (int64(intervalIdx) * intervalSeconds)
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
	aggs.TxfeeAggregates = models.TxfeeAggregates{StartTime: startTime, EndTime: endTime}
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

	aggs.StartTime = startTime
	aggs.EndTime = endTime

	ac.aggregateFeesMap[chainid][rangeKeyType] = aggs.TxfeeAggregates.Txfee
	return nil
}

func getFirstTransactionTime(conns *utils.Connections, chainIDs []string) (time.Time, error) {
	dbRunner, err := conns.DB().NewSession("get_first_transaction_time", cfg.RequestTimeout)
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

	err = builder.LoadOne(&ts)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(int64(math.Floor(ts)), 0).UTC(), nil
}
