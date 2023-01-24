##
## Multisig alias
##
create table multisig_aliases (
    alias           char(33) not null,
    owner           char(33) not null,
    transaction_id  char(49) not null,
    created_at      timestamp   not null default current_timestamp,
    primary key(alias, owner)
);

create index multisig_aliases_alias on multisig_aliases (alias);

create index multisig_aliases_owner on multisig_aliases (owner);


