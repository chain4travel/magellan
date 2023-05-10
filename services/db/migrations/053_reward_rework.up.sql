DROP TABLE rewards;
DROP TABLE transactions_rewards_owners;
DROP TABLE transactions_rewards_owners_address;
DROP TABLE transactions_rewards_owners_outputs;

CREATE TABLE `reward_owner` (
  address varchar(50) not null,
  hash varchar(50) not null,
  created_at timestamp not null default current_timestamp,
  PRIMARY KEY (address, hash)
);

CREATE TABLE `reward` (
  reward_owner_bytes VARBINARY(64000),
  reward_owner_hash varchar(50) not null,
  tx_id varchar(50) not null,
  type tinyint not null default 0,
  updated_at timestamp not null default current_timestamp,
  PRIMARY KEY (reward_owner_hash, tx_id, type)
);

CREATE INDEX reward_owner_address ON reward_owner (address);
CREATE INDEX reward_owner_hash ON reward_owner (hash);

CREATE INDEX reward_reward_owner_hash ON reward (reward_owner_hash);
