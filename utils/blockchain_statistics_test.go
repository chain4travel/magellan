package utils

import (
	"testing"

	"github.com/chain4travel/magellan/models"
	"github.com/stretchr/testify/require"
)

type unionStatisticsArgs struct {
	cvmLatestTx    []*models.CvmStatisticsCache
	avmLatestTx    []*models.AvmStatisticsCache
	cvmLatestBlock []*models.CvmBlocksStatisticsCache
	AddressesFrom  []*models.AddressesCache
	AddressesTo    []*models.AddressesCache
}

func TestUnionStatistics(t *testing.T) {
	require := require.New(t)
	tests := map[string]struct {
		args unionStatisticsArgs
		want []models.StatisticsCache
	}{
		"allEmptyStatistics": {
			args: unionStatisticsArgs{
				cvmLatestTx:    []*models.CvmStatisticsCache{},
				avmLatestTx:    []*models.AvmStatisticsCache{},
				cvmLatestBlock: []*models.CvmBlocksStatisticsCache{},
				AddressesFrom:  []*models.AddressesCache{},
				AddressesTo:    []*models.AddressesCache{},
			},
			want: []models.StatisticsCache{},
		},
		"onlyCvmTxStatistics": {
			args: unionStatisticsArgs{
				cvmLatestTx: []*models.CvmStatisticsCache{{
					DateAt:        "2023-04-10",
					CvmTx:         134568263,
					ReceiveCount:  213,
					SendCount:     465,
					Blocks:        30,
					GasPrice:      123456798.12123,
					GasUsed:       2178946514.45678,
					TokenTransfer: 12345678.124,
				}},
				avmLatestTx:    []*models.AvmStatisticsCache{},
				cvmLatestBlock: []*models.CvmBlocksStatisticsCache{},
				AddressesFrom:  []*models.AddressesCache{},
				AddressesTo:    []*models.AddressesCache{},
			},
			want: []models.StatisticsCache{{
				DateAt:        "2023-04-10",
				CvmTx:         134568263,
				ReceiveCount:  213,
				SendCount:     465,
				Blocks:        30,
				GasPrice:      123456798.12123,
				GasUsed:       2178946514.45678,
				TokenTransfer: 12345678.124,
			}},
		},
		"onlyAvmTxStatistics": {
			args: unionStatisticsArgs{
				cvmLatestTx: []*models.CvmStatisticsCache{},
				avmLatestTx: []*models.AvmStatisticsCache{{
					DateAt: "2023-04-10",
					AvmTx:  123456,
				}},
				cvmLatestBlock: []*models.CvmBlocksStatisticsCache{},
				AddressesFrom:  []*models.AddressesCache{},
				AddressesTo:    []*models.AddressesCache{},
			},
			want: []models.StatisticsCache{{
				DateAt: "2023-04-10",
				AvmTx:  123456,
			}},
		},
		"onlyCvmBlockStatistics": {
			args: unionStatisticsArgs{
				cvmLatestTx: []*models.CvmStatisticsCache{},
				avmLatestTx: []*models.AvmStatisticsCache{},
				cvmLatestBlock: []*models.CvmBlocksStatisticsCache{{
					DateAt:       "2023-04-10",
					AvgBlockSize: 1024,
				}},
				AddressesFrom: []*models.AddressesCache{},
				AddressesTo:   []*models.AddressesCache{},
			},
			want: []models.StatisticsCache{{
				DateAt:       "2023-04-10",
				AvgBlockSize: 1024,
			}},
		},
		"onlySendAddressesStatistics": {
			args: unionStatisticsArgs{
				cvmLatestTx: []*models.CvmStatisticsCache{{
					DateAt: "2023-04-10",
				}},
				avmLatestTx:    []*models.AvmStatisticsCache{},
				cvmLatestBlock: []*models.CvmBlocksStatisticsCache{},
				AddressesFrom: []*models.AddressesCache{{
					Address: "2",
					DateAt:  "2023-04-10",
				}},
				AddressesTo: []*models.AddressesCache{},
			},
			want: []models.StatisticsCache{{
				ActiveAccounts: 1,
				DateAt:         "2023-04-10",
			}},
		},
		"onlyReceiveAddressesStatistics": {
			args: unionStatisticsArgs{
				cvmLatestTx: []*models.CvmStatisticsCache{{
					DateAt: "2023-04-10",
				}},
				avmLatestTx:    []*models.AvmStatisticsCache{},
				cvmLatestBlock: []*models.CvmBlocksStatisticsCache{},
				AddressesFrom:  []*models.AddressesCache{},
				AddressesTo: []*models.AddressesCache{{
					Address: "1",
					DateAt:  "2023-04-10",
				}},
			},
			want: []models.StatisticsCache{{
				ActiveAccounts: 1,
				DateAt:         "2023-04-10",
			}},
		},
	}

	for name, tt := range tests {
		testCase := tt
		t.Run(name, func(t *testing.T) {
			require.EqualValues(testCase.want, UnionStatistics(testCase.args.cvmLatestTx, testCase.args.cvmLatestBlock,
				testCase.args.avmLatestTx, testCase.args.AddressesFrom, testCase.args.AddressesTo))
		})
	}
}
