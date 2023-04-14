package utils

import (
	"reflect"
	"testing"

	"github.com/chain4travel/magellan/models"
)

func TestUnionStatistics(t *testing.T) {
	mockCvmLatestTx := []*models.CvmStatisticsCache{}
	mockAvmLatesTx := []*models.AvmStatisticsCache{}
	mockCvmLatestBlock := []*models.CvmBlocksStatisticsCache{}

	// validate if all statistics are empty
	want := []models.StatisticsCache{}
	got := UnionStatistics(mockCvmLatestTx, mockCvmLatestBlock, mockAvmLatesTx)
	if !reflect.DeepEqual(want, got) {
		t.Errorf("Expected size: %v, got: %v", want, got)
	}

	// validate if only have the cvm latest tx statistics
	mockCvmLatestTx = append(mockCvmLatestTx, &models.CvmStatisticsCache{
		DateAt:         "2023-04-10",
		CvmTx:          134568263,
		ReceiveCount:   213,
		SendCount:      465,
		ActiveAccounts: 465,
		Blocks:         30,
		GasPrice:       123456798.12123,
		GasUsed:        2178946514.45678,
		TokenTransfer:  12345678.124,
	})

	want = append(want, models.StatisticsCache{
		DateAt:         "2023-04-10",
		CvmTx:          134568263,
		ReceiveCount:   213,
		SendCount:      465,
		ActiveAccounts: 465,
		Blocks:         30,
		GasPrice:       123456798.12123,
		GasUsed:        2178946514.45678,
		TokenTransfer:  12345678.124,
	})

	got = UnionStatistics(mockCvmLatestTx, mockCvmLatestBlock, mockAvmLatesTx)
	if !reflect.DeepEqual(want, got) {
		t.Errorf("Expected size: %v, got: %v", want, got)
	}

	// validate if only have the cvm latest tx and cvm latest blocks statistics
	mockCvmLatestBlock = append(mockCvmLatestBlock, &models.CvmBlocksStatisticsCache{
		DateAt:       "2023-04-10",
		AvgBlockSize: 1024,
	})

	want[0].AvgBlockSize = 1024
	got = UnionStatistics(mockCvmLatestTx, mockCvmLatestBlock, mockAvmLatesTx)
	if !reflect.DeepEqual(want, got) {
		t.Errorf("Expected size: %v, got: %v", want, got)
	}

	// validate using all the statistics
	mockAvmLatesTx = append(mockAvmLatesTx, &models.AvmStatisticsCache{
		DateAt: "2023-04-10",
		AvmTx:  123456,
	})

	want[0].AvmTx = 123456
	got = UnionStatistics(mockCvmLatestTx, mockCvmLatestBlock, mockAvmLatesTx)
	if !reflect.DeepEqual(want, got) {
		t.Errorf("Expected size: %v, got: %v", want, got)
	}
}