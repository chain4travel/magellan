package utils

import (
	"strings"

	"github.com/chain4travel/magellan/models"
)

func UnionStatistics(
	cvmLatestTx []*models.CvmStatisticsCache,
	cvmLatestBlocks []*models.CvmBlocksStatisticsCache,
	avmLatestTx []*models.AvmStatisticsCache) []models.StatisticsCache {
	statisticsCache := []models.StatisticsCache{}
	// set all the information from cvmTx in statisticsCache variable
	setCvmTxStatistics(&statisticsCache, cvmLatestTx)
	setCvmBlockStatistics(&statisticsCache, cvmLatestBlocks)
	setAvmTxStatistics(&statisticsCache, avmLatestTx)
	return statisticsCache
}

func setCvmTxStatistics(statistics *[]models.StatisticsCache, cvmLatestTx []*models.CvmStatisticsCache) {
	for _, cvmTx := range cvmLatestTx {
		dateAt := strings.Split(cvmTx.DateAt, "T")[0]
		*statistics = append(*statistics,
			models.StatisticsCache{
				DateAt:         dateAt,
				CvmTx:          cvmTx.CvmTx,
				ReceiveCount:   cvmTx.ReceiveCount,
				SendCount:      cvmTx.SendCount,
				ActiveAccounts: cvmTx.ActiveAccounts,
				Blocks:         cvmTx.Blocks,
				GasPrice:       cvmTx.GasPrice,
				GasUsed:        cvmTx.GasUsed,
				TokenTransfer:  cvmTx.TokenTransfer,
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
