DROP INDEX cvm_transactions_txdata_block_idx on cvm_transactions_txdata;
ALTER TABLE cvm_transactions_txdata DROP COLUMN block_idx;
CREATE UNIQUE INDEX cvm_transactions_txdata_block ON cvm_transactions_txdata (block, idx);

DROP INDEX cvm_transactions_atomic_block_idx on cvm_transactions_atomic;
ALTER TABLE cvm_transactions_atomic DROP COLUMN block_idx;
CREATE UNIQUE INDEX cvm_transactions_atomic_block ON cvm_transactions_atomic (block, idx);