// Copyright (C) 2022, Chain4Travel AG. All rights reserved.
//
// This file is a derived work, based on ava-labs code.
//
// It is distributed under the same license conditions as the
// original code from which it is derived.
//
// Much love to the original authors for their work.
// **********************************************************

package avax

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/chain4travel/magellan/cfg"
	"github.com/chain4travel/magellan/db"
	"github.com/chain4travel/magellan/models"
	"github.com/chain4travel/magellan/services/indexes/params"
	"github.com/chain4travel/magellan/utils"
	"github.com/gocraft/dbr/v2"
)

func (r *Reader) ListCBlocks(ctx context.Context, p *params.ListCBlocksParams) (*models.CBlockList, error) {
	fmtHex := func(n uint64) string { return "0x" + strconv.FormatUint(n, 16) }

	dbRunner, err := r.conns.DB().NewSession("list_cblocks", cfg.RequestTimeout)
	if err != nil {
		return nil, err
	}

	result := models.CBlockList{}

	// Step 1 get Block headers
	if p.ListParams.Limit > 0 {
		var blockList []*db.CvmBlocks

		sq := dbRunner.Select(
			"evm_tx",
			"atomic_tx",
			"serialization",
		).
			From(db.TableCvmBlocks)

		if p.ListParams.StartTimeProvided {
			sq = sq.Where("created_at >= ?", p.ListParams.StartTime)
		}
		if p.ListParams.EndTimeProvided {
			sq = sq.Where("created_at < ?", p.ListParams.EndTime)
		}

		switch {
		case p.BlockStart != nil:
			sq = sq.OrderDesc("block").
				Where("block <= ?", p.BlockStart.Uint64())
		case p.BlockEnd != nil:
			sq = sq.OrderAsc("block").
				Where("block >= ?", p.BlockEnd.Uint64())
		default:
			sq = sq.OrderDesc("block")
		}

		sq = sq.Limit(uint64(p.ListParams.Limit))
		_, err = sq.LoadContext(ctx, &blockList)
		if err != nil {
			return nil, err
		}

		result.Blocks = make([]*models.CBlockHeaderBase, len(blockList))
		for i, block := range blockList {
			err = json.Unmarshal(block.Serialization, &result.Blocks[i])
			if err != nil {
				return nil, err
			}
			result.Blocks[i].EvmTx = block.EvmTx
			result.Blocks[i].AtomicTx = block.AtomicTx
		}
	}

	// Step 2 get Transactions
	if p.TxLimit > 0 {
		var txList []*struct {
			Serialization []byte
			CreatedAt     time.Time
			FromAddr      string
			Block         uint64
			Idx           uint64
			Status        uint16
			GasUsed       uint64
			GasPrice      uint64
		}

		sq := dbRunner.Select(
			"serialization",
			"created_at",
			"F.address AS from_addr",
			"block",
			"idx",
			"status",
			"gas_used",
			"gas_price",
			"block_idx",
		).
			From(db.TableCvmTransactionsTxdata).
			LeftJoin(dbr.I(db.TableCvmAccounts).As("F"), "id_from_addr = F.id")

		if p.ListParams.StartTimeProvided {
			sq = sq.Where("created_at >= ?", p.ListParams.StartTime)
		}
		if p.ListParams.EndTimeProvided {
			sq = sq.Where("created_at < ?", p.ListParams.EndTime)
		}

		switch {
		case p.BlockStart != nil:
			sq = sq.OrderDesc("block_idx").
				Where("block_idx <= ?", p.BlockStart.Uint64()*1000+999-uint64(p.TxID))
		case p.BlockEnd != nil:
			sq = sq.OrderAsc("block_idx").
				Where("block_idx >= ?", p.BlockEnd.Uint64()*1000+999-uint64(p.TxID))
		default:
			sq = sq.OrderDesc("block_idx").
				Where("block_idx IS NOT NULL")
		}

		if len(p.CAddresses) > 0 {
			sq = sq.Distinct()
			addressesSQL := strings.Join(p.CAddresses, "','")
			addressesSQL = "'" + addressesSQL + "'"
			sq = sq.From("(select id from cvm_accounts where address in (" + addressesSQL + ") ) sub,cvm_transactions_txdata")
			sq = sq.Where("(id_from_addr=sub.id  OR id_to_addr=sub.id)")
		}

		sq = sq.Limit(uint64(p.TxLimit))
		_, err = sq.LoadContext(ctx, &txList)
		if err != nil {
			return nil, err
		}

		result.Transactions = make([]*models.CTransactionDataBase, len(txList))
		for i, tx := range txList {
			dest := &result.Transactions[i]
			err = json.Unmarshal(tx.Serialization, dest)
			if err != nil {
				return nil, err
			}
			(*dest).Block = fmtHex(tx.Block)
			(*dest).Index = fmtHex(tx.Idx)
			(*dest).CreatedAt = fmtHex(uint64(tx.CreatedAt.Unix()))
			(*dest).From = tx.FromAddr
			(*dest).Status = fmtHex(uint64(tx.Status))
			(*dest).GasUsed = fmtHex(tx.GasUsed)
			(*dest).EffectiveGasPrice = fmtHex(tx.GasPrice)
		}
	}

	return &result, nil
}

func (r *Reader) AverageBlockSizeReader(ctx context.Context, p *params.ListParams) ([]*models.AverageBlockSize, error) {
	dbRunner, err := r.conns.DB().NewSession("average_block_size", cfg.RequestTimeout)
	var averageBlockSizeData []*models.AverageBlockSize
	var baseq *dbr.SelectStmt
	var dateFormat string

	if err != nil {
		return []*models.AverageBlockSize{}, err
	}
	/*
		If the limit has not been established in the parameters, the query will be carried out with a daily
		filter ("YYYY-MM-DD" format), if it is not, it will depend on the date range that is sent
		(Daily: "YYYY-MM-DD", Monthly: "YYYY-MM-01" or Yearly: "YYYY-01-01")

		sending the start date in the two parameters of the DateFormat function forces to obtain
		the daily grouping filter.
	*/
	if p.Limit > 0 {
		dateFormat = utils.DateFormat(p.StartTime, p.StartTime, "date_at")
	} else {
		dateFormat = utils.DateFormat(p.StartTime, p.EndTime, "date_at")
	}

	baseq = dbRunner.Select("AVG(avg_block_size) AS block_size", dateFormat+" as date_info").
		From("statistics").
		Where("date_at BETWEEN ? AND ?", p.StartTime, p.EndTime).
		GroupBy(dateFormat)

	if p.Limit > 0 {
		baseq.Limit(uint64(p.Limit))
	}

	_, err = baseq.LoadContext(ctx, &averageBlockSizeData)

	if err != nil || len(averageBlockSizeData) == 0 {
		return []*models.AverageBlockSize{}, err
	}

	return averageBlockSizeData, err
}
