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
	"time"

	"github.com/chain4travel/magellan/cfg"
	"github.com/chain4travel/magellan/db"
	"github.com/chain4travel/magellan/models"
	"github.com/chain4travel/magellan/services/indexes/params"
)

func (r *Reader) ListCBlocks(ctx context.Context, p *params.ListCBlocksParams) (*models.CBlockList, error) {
	fmtHex := func(n uint64) string { return "0x" + strconv.FormatUint(n, 16) }

	dbRunner, err := r.conns.DB().NewSession("list_cblocks", cfg.RequestTimeout)
	if err != nil {
		return nil, err
	}

	result := models.CBlockList{}
	err = dbRunner.Select("COUNT(block)").
		From(db.TableCvmBlocks).
		LoadOneContext(ctx, &result.BlockCount)
	if err != nil {
		return nil, err
	}

	err = dbRunner.Select("COUNT(hash)").
		From(db.TableCvmTransactionsTxdata).
		LoadOneContext(ctx, &result.TransactionCount)
	if err != nil {
		return nil, err
	}

	// Setp 1 get Block headers
	if p.ListParams.Limit > 0 {
		var blockList []*db.CvmBlocks

		sq := dbRunner.Select(
			"evm_tx",
			"atomic_tx",
			"serialization",
		).
			From(db.TableCvmBlocks).
			Limit(uint64(p.ListParams.Limit))

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

	// Setp 2 get Transactions
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
			"from_addr",
			"block",
			"idx",
			"status",
			"gas_used",
			"gas_price",
		).
			From(db.TableCvmTransactionsTxdata).
			OrderDesc("block_idx").
			Limit(uint64(p.TxLimit))

		switch {
		case p.BlockStart != nil:
			sq = sq.OrderDesc("block_idx").
				Where("block_idx <= ?", p.BlockStart.Uint64()*1000+999-uint64(p.TxID))
		case p.BlockEnd != nil:
			sq = sq.OrderAsc("block_idx").
				Where("block_idx >= ?", p.BlockEnd.Uint64()*1000+999-uint64(p.TxID))
		default:
			sq = sq.OrderDesc("block_idx")
		}

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
