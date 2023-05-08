package utils

import (
	"strings"

	"github.com/chain4travel/magellan/models"
)

func UnionStatistics(
	cvmLatestTx []*models.CvmStatisticsCache,
	cvmLatestBlocks []*models.CvmBlocksStatisticsCache,
	avmLatestTx []*models.AvmStatisticsCache,
	addressesFrom []*models.AddressesCache,
	addressesTo []*models.AddressesCache) []models.StatisticsCache {
	statisticsCache := []models.StatisticsCache{}
	// set all the information from cvmTx in statisticsCache variable
	setCvmTxStatistics(&statisticsCache, cvmLatestTx)
	setCvmBlockStatistics(&statisticsCache, cvmLatestBlocks)
	setAvmTxStatistics(&statisticsCache, avmLatestTx)
	setActiveAddressStatistics(&statisticsCache, addressesFrom, addressesTo)
	return statisticsCache
}

func setCvmTxStatistics(statistics *[]models.StatisticsCache, cvmLatestTx []*models.CvmStatisticsCache) {
	for _, cvmTx := range cvmLatestTx {
		dateAt := strings.Split(cvmTx.DateAt, "T")[0]
		*statistics = append(*statistics,
			models.StatisticsCache{
				DateAt:        dateAt,
				CvmTx:         cvmTx.CvmTx,
				ReceiveCount:  cvmTx.ReceiveCount,
				SendCount:     cvmTx.SendCount,
				Blocks:        cvmTx.Blocks,
				GasPrice:      cvmTx.GasPrice,
				GasUsed:       cvmTx.GasUsed,
				TokenTransfer: cvmTx.TokenTransfer,
			})
	}
}

func setCvmBlockStatistics(statistics *[]models.StatisticsCache, cvmBlocks []*models.CvmBlocksStatisticsCache) {
	for _, cvmBlock := range cvmBlocks {
		dateAt := strings.Split(cvmBlock.DateAt, "T")[0]
		statisticIndex := getLatestTxIndex(statistics, dateAt)
		if statisticIndex < 0 {
			*statistics = append(*statistics,
				models.StatisticsCache{
					DateAt:       dateAt,
					AvgBlockSize: cvmBlock.AvgBlockSize,
				})
		} else {
			(*statistics)[statisticIndex].AvgBlockSize = cvmBlock.AvgBlockSize
		}
	}
}

func setAvmTxStatistics(statistics *[]models.StatisticsCache, avmLatestTx []*models.AvmStatisticsCache) {
	for _, avmTx := range avmLatestTx {
		dateAt := strings.Split(avmTx.DateAt, "T")[0]
		statisticIndex := getLatestTxIndex(statistics, dateAt)
		if statisticIndex < 0 {
			*statistics = append(*statistics,
				models.StatisticsCache{
					DateAt: dateAt,
					AvmTx:  avmTx.AvmTx,
				})
		} else {
			(*statistics)[statisticIndex].AvmTx = avmTx.AvmTx
		}
	}
}

func getLatestTxIndex(statistics *[]models.StatisticsCache, date string) int {
	for idx, statistic := range *statistics {
		if statistic.DateAt == date {
			return idx
		}
	}
	return -1
}

func getActiveAddresses(addressesFrom []*models.AddressesCache, addressesTo []*models.AddressesCache) []*models.AddressesCache {
	unionActive := addressesFrom
	for _, address := range addressesTo {
		if getLatestAddrIndex(unionActive, address.Address, address.DateAt) < 0 {
			unionActive = append(unionActive, address)
		}
	}
	return unionActive
}

func setActiveAddressStatistics(statistics *[]models.StatisticsCache, addressesFrom []*models.AddressesCache, addressesTo []*models.AddressesCache) {
	union := getActiveAddresses(addressesFrom, addressesTo)
	for _, Addr := range union {
		idx := getLatestTxIndex(statistics, strings.Split(Addr.DateAt, "T")[0])
		if idx >= 0 {
			(*statistics)[idx].ActiveAccounts++
		}
	}
}

func getLatestAddrIndex(addresses []*models.AddressesCache, addressTo string, date string) int {
	for idx, address := range addresses {
		if address.DateAt == date && address.Address == addressTo {
			return idx
		}
	}
	return -1
}
