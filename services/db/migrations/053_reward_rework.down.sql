DROP TABLE IF EXISTS reward_owner;
DROP TABLE IF EXISTS reward;

create table transactions_rewards_owners
(
    id varchar(50) not null primary key,
    chain_id varchar(50) not null,
    locktime bigint unsigned not null,
    threshold int unsigned not null,
    created_at timestamp(6) default CURRENT_TIMESTAMP(6) not null
);

create table transactions_rewards_owners_address
(
    id varchar(50) not null,
    address varchar(50) not null,
    output_index smallint unsigned not null,
    primary key (id, address),
    updated_at timestamp(6) default CURRENT_TIMESTAMP(6) not null
);

create index transactions_rewards_owners_address_address on transactions_rewards_owners_address (address);

create table transactions_rewards_owners_outputs
(
    id varchar(50) not null primary key,
    transaction_id varchar(50)       not null,
    output_index int unsigned not null,
    created_at timestamp(6) default CURRENT_TIMESTAMP(6) not null
);

create table `rewards`
(
    id varchar(50) not null primary key,
    block_id varchar(50) not null,
    txid varchar(50) not null,
    shouldprefercommit smallint unsigned default 0,
    created_at timestamp(6) not null default current_timestamp(6),
    processed smallint unsigned default 0
);

create index rewards_block_id ON rewards (block_id);
create index rewards_txid ON rewards (txid);
create index rewards_processed on rewards (processed, created_at);