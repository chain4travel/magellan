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
