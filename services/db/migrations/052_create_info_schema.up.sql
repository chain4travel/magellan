-- magellan.Test definition

CREATE TABLE `statistics` (
  `date_at` date NOT NULL,
  `cvm_tx` int DEFAULT NULL,
  `avm_tx` int DEFAULT NULL,
  `token_transfer` float DEFAULT NULL,
  `gas_used` float DEFAULT NULL,
  `receive_count` int DEFAULT NULL,
  `send_count` int DEFAULT NULL,
  `active_accounts` int DEFAULT NULL,
  `gas_price` float DEFAULT NULL,
  `blocks` int DEFAULT NULL,
  `avg_block_size`float DEFAULT NULL,
  PRIMARY KEY (`date_at`)
);

CREATE PROCEDURE `daily_statistics_update`()
BEGIN
DECLARE v_created_at DATE;
DECLARE v_cvm_tx INTEGER;
DECLARE v_avm_tx INTEGER;
DECLARE v_receive_count INTEGER;
DECLARE v_send_count INTEGER;
DECLARE v_active_accounts INTEGER;
DECLARE v_token_transfer FLOAT;
DECLARE v_gas_used FLOAT;
DECLARE v_gas_price FLOAT;
DECLARE v_blocks INTEGER;
DECLARE v_avg_block_size FLOAT;

-- 1. create a cursor to save the results of date, cvm tx count, avm tx count, addresses send count, addresses receive count,
--    total token transfer, total gas price, total gas used, avg block size, blocks count... using a full join between
--    cvm_transactions_txdata, avm_transactions and cvm_blocks
DECLARE cur1 CURSOR FOR WITH cvm_transaction_results AS (
    SELECT DATE(created_at) as date_at, COUNT(*) as cvm_tx, COUNT(DISTINCT id_to_addr) receive_count, 
    COUNT(DISTINCT id_from_addr) send_count, SUM(amount) as token_transfer, SUM(gas_used) as gas_used, SUM(gas_price) as gas_price,
    COUNT(DISTINCT block_idx) as blocks
    FROM cvm_transactions_txdata ctt 
    WHERE DATE(created_at) >= @max_date
    GROUP BY DATE(created_at)
    ),
    avm_transaction_results AS (
        SELECT DATE(created_at) as date_at_avm, COUNT(*) as avm_tx
        FROM avm_transactions at2 
        WHERE DATE(created_at) >= @max_date
        GROUP BY DATE(created_at)
    ),
    cvm_blocks_results AS (
        SELECT DATE(created_at) as date_at_blocks, AVG(size) as avg_size
        FROM cvm_blocks cb
        WHERE DATE(created_at) >= @max_date
        GROUP BY DATE(created_at)
    )
    SELECT *
    FROM (
        SELECT t1.cvm_tx, t1.date_at, t1.token_transfer, t1.gas_used, t1.receive_count, t1.send_count, GREATEST(t1.receive_count, t1.send_count), t1.gas_price, t1.blocks, t2.avm_tx, t3.avg_size
        FROM cvm_transaction_results t1
        LEFT JOIN avm_transaction_results t2 ON t1.date_at = t2.date_at_avm
        LEFT JOIN cvm_blocks_results t3 ON t1.date_at = t3.date_at_blocks
        UNION
        SELECT t1.cvm_tx, t2.date_at_avm as date_at, t1.token_transfer, t1.gas_used,t1.receive_count, t1.send_count,GREATEST(t1.receive_count, t1.send_count), t1.gas_price, t1.blocks, t2.avm_tx, t3.avg_size
        FROM avm_transaction_results t2
        LEFT JOIN cvm_transaction_results t1 ON t2.date_at_avm = t1.date_at
        LEFT JOIN cvm_blocks_results t3 ON t2.date_at_avm = t3.date_at_blocks
    ) as results;

 -- 2. Error control in case that the statistics table not exists.
CREATE TABLE IF NOT EXISTS statistics (
    date_at DATE,
    cvm_tx INT,
    avm_tx INT,
    receive_count INT,
    send_count INT,
    gas_price INT,
    blocks INT,
    token_transfer FLOAT,
    gas_used FLOAT,
    gas_price FLOAT,
    avg_block_size FLOAT,
    PRIMARY KEY (date_at)
   );

-- 3. Search the max date in statistics table used as condition in the cursor
  SELECT
	MAX(date_at)
INTO
	@max_date
FROM
	statistics;

-- 3.1 if the table test is empty, set the max date for default with 0000-00-00
  IF @max_date IS NULL THEN
    SET @max_date = '1000-01-01';
  END IF;

  OPEN cur1;
  FETCH cur1 INTO v_cvm_tx,v_created_at,v_token_transfer, v_gas_used, v_receive_count, v_send_count, v_active_accounts,v_gas_price, v_blocks, v_avm_tx, v_avg_block_size;
-- 4. Insert or update the data in statistics table
  WHILE (v_created_at IS NOT NULL) DO
  
  -- 4.1 if the current date in v_created_at is equal to another value in the table statistics update de data of this row
   IF EXISTS(SELECT 1 FROM statistics WHERE date_at = v_created_at) THEN
        UPDATE statistics
        SET cvm_tx = v_cvm_tx, avm_tx = v_avm_tx, token_transfer = v_token_transfer, gas_used = v_gas_used, receive_count = v_receive_count,
        send_count = v_send_count, active_accounts = v_active_accounts, gas_price = v_gas_price, blocks = v_blocks, avg_block_size = v_avg_block_size
        WHERE date_at = v_created_at;
    ELSE
  -- 4.2 if not exist a previous value in the statistics table with this date insert all the data.
       INSERT INTO statistics (date_at, cvm_tx, avm_tx, token_transfer, gas_used, receive_count, send_count,active_accounts,gas_price,blocks, avg_block_size)
       VALUES(v_created_at, v_cvm_tx, v_avm_tx,v_token_transfer, v_gas_used, v_receive_count, v_send_count, v_active_accounts,v_gas_price, v_blocks, v_avg_block_size);

    END IF;

   FETCH cur1 INTO v_cvm_tx,v_created_at,v_token_transfer, v_gas_used, v_receive_count, v_send_count, v_active_accounts,v_gas_price, v_blocks, v_avm_tx, v_avg_block_size;
  END WHILE;
  
  CLOSE cur1;
END;