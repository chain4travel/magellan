##
## Multisig alias
##
create table multisig_aliases (
    alias           varchar(35) not null,
    owner           varchar(35) not null,
    transaction_id  varchar(56) not null,
    created_at      timestamp   not null default current_timestamp,
    primary key(alias, owner)
);

create index multisig_aliases_alias on multisig_aliases (alias);

create index multisig_aliases_owner on multisig_aliases (owner);


