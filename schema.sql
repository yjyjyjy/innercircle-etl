-- contract data from google bigquery
create table eth_contracts (
	address	varchar primary key
	, is_erc20 BOOLEAN
	, is_erc721 BOOLEAN
	, is_nft BOOLEAN
	, timestamp TIMESTAMP
);


-- all ethererum transaction data
create table eth_transactions (
    timestamp timestamp
    , trx_hash varchar(100) primary key
    , from_address varchar(100)
    , to_address varchar(100)
    , eth_value numeric
);


-- the transfer of ERC-20 and ERC-721 tokens
create table eth_token_transfers (
	timestamp timestamp
	, trx_hash varchar
	, contract varchar
	, from_address varchar
	, to_address varchar
	, token_id_or_value varchar
)
;

create table eth_token_transfers_2017 as
select * from eth_token_transfers where timestamp >= '2017-01-01' and timestamp < '2018-01-01';
create table eth_token_transfers_2018 as
select * from eth_token_transfers where timestamp >= '2018-01-01' and timestamp < '2019-01-01';
create table eth_token_transfers_2019 as
select * from eth_token_transfers where timestamp >= '2019-01-01' and timestamp < '2020-01-01';
create table eth_token_transfers_2020 as
select * from eth_token_transfers where timestamp >= '2020-01-01' and timestamp < '2021-01-01';
create table eth_token_transfers_2021 as
select * from eth_token_transfers where timestamp >= '2021-01-01' and timestamp < '2022-01-01';
create table eth_token_transfers_2022 as
select * from eth_token_transfers where timestamp >= '2022-01-01' and timestamp < '2023-01-01';


-- decoded trading information that happened on OpenSea etc.
create table nft_trades (
	timestamp timestamp
	, trx_hash varchar primary key
	, eth_value numeric
	, payment_token varchar
	, price numeric
	, platform varchar(50)
)
;
create index nft_trades_idx_timestamp on nft_trades (timestamp desc);

-- this table is to trigger backfill for newly identified NFT contracts
create table new_nft_contracts (
	address varchar primary key
	, missing_metadata BOOLEAN
	, missing_trx_union BOOLEAN
);


create table collection (
        id varchar primary key, -- contract address
        name varchar not null, -- collection name
        safelist_request_status varchar, -- verified/approved 💎 if verified, there is a blue checkmark on opensea
        description varchar,
        image_url varchar,
        banner_image_url varchar,
        external_url varchar, -- usually website
        twitter_username varchar,
        discord_url varchar,
        telegram_url varchar,
        instagram_username varchar,
        medium_username varchar,
        wiki_url varchar,
        payout_address varchar,
        slug varchar -- not sure what this is
);

-- create table nft_contract_abi (
-- 	address varchar primary key
-- 	, abi varchar
-- );


-- All transactions of the NFT tokens including trade, mint, airdrops
/*
re: num_tokens_in_the_same_transaction, and eth_value_per_token
Say I bought 3 NFTs in one transactions for 3 eth: 2 eth for token A; 0.5 eth for token B and C each). There will be three rows in this table with the same trx_hash. The num_tokens_in_the_same_transaction == 3 and eth_value_per_token == 1 for all three rows.
*/
create table nft_trx_union (
	timestamp timestamp
	, trx_hash varchar
	, contract varchar
	, token_id varchar
	, from_address varchar
	, to_address varchar
	, trade_platform varchar
	, trade_payment_token varchar
	, num_tokens_in_the_same_transaction int
	, price_per_token numeric
	, action varchar --
	, caller_is_receiver BOOLEAN -- meaning the wallet received the token initiated the call
)
;
create index nft_trx_union_idx_timestamp on nft_trx_union (timestamp desc);
create index nft_trx_union_idx_contract_token_id on nft_trx_union (contract, token_id desc);
create index nft_trx_union_idx_from_address on nft_trx_union (from_address);
create index nft_trx_union_idx_to_address on nft_trx_union (to_address);
create index nft_trx_union_idx_action on nft_trx_union (action);

create table nft_ownership (
	contract varchar,
	token_id varchar,
	owner varchar,
	PRIMARY KEY(contract, token_id)
);

create table nft_contract_floor_price (
	date date,
	contract varchar,
	floor_price_in_eth numeric,
	primary key (date, contract)
)
;

drop table if exists owner_collection_worth;
create table owner_collection_worth as
select
	o.owner
	, o.contract
	, p.floor_price_in_eth
	, count(distinct o.token_id) as collection_value
from nft_ownership o
join nft_contract_floor_price p
	on p.contract = o.contract
where p.date = '2022-01-03'-- {yesterday}
group by 1,2,3
;

create table owner_collection_total_worth as
with base as (
	select
		o.owner
		, o.contract
		, p.floor_price_in_eth
		, count(distinct o.token_id) as num_tokens
	from nft_ownership o
	join nft_contract_floor_price p
		on p.contract = o.contract
	where p.date = '2022-01-03'-- {yesterday}
	group by 1,2,3
)
, owner_collection_worth as (
	select
		owner
		, contract
		, floor_price_in_eth * num_tokens as collection_worth
	from base
)
, owner_collection_worth_ranked as (
	select
		owner
		, contract
		, collection_worth
		, row_number() over (partition by owner order by collection_worth desc) as rnk
	from owner_collection_worth
)
, top_collection_weight as (
	select
		owner
		, sum(case when rnk = 1 then collection_worth end)/sum(collection_worth) as top_collection_weight
	from owner_collection_worth_ranked
	group by 1
)
, owner_worth as (
	select
		owner
		, sum(collection_worth) as total_worth
	from owner_collection_worth
	group by 1
)
select
	ocwr.owner
	, ocwr.contract
	, ocwr.collection_worth
	, rnk
	, total_worth
from owner_collection_worth_ranked ocwr
join owner_worth ow
	on ocwr.owner = ow.owner
;

-- mapping logic from insider to circle
create table insider_to_circle_mapping (
	insider_id varchar not null
	, owner_rank int not null
	, circle_id int not null
	, created_at date not null
	, is_current BOOLEAN not null
	, foreign key (insider_id)  references insider(id)
	, foreign key (circle_id)  references circle(id)
);
create unique index "insider_to_circle_mapping_unique_idx_insider_id_circle_id_created_at" ON insider_to_circle_mapping(insider_id, circle_id, created_at);
create index insider_to_circle_mapping_idx_is_current on insider_to_circle_mapping (is_current);

create table circle (
	id SERIAL PRIMARY KEY
	, name varchar
);

create table insider (
	id varchar PRIMARY KEY
	, ens varchar
	, twitter_username varchar
	, instagram_username varchar
);

create table address_metadata (
	id varchar primary key
	, public_name_tag varchar
	, is_contract BOOLEAN
	, is_special_address boolean
	, special_account_type varchar
	, opensea_user_created_at timestamp
	, opensea_display_name varchar
	, opensea_banner_image_url varchar
	, opensea_image_url varchar
	, opensea_bio varchar
	, ens varchar
	, twitter_username varchar
	, instagram_username varchar
	, medium_username varchar
	, email varchar
	, website varchar
)
;
create index address_metadata_idx_email on address_metadata (email);
create index address_metadata_idx_is_contract on address_metadata (is_contract);
create index address_metadata_idx_is_special_address on address_metadata (is_special_address);

-- get all collection bought or mint by the insiders and the first date for each collection/insider pair
create table insight_trx (
	insider_id varchar not null -- eth address
	, collection_id varchar not null
	, timestamp timestamp not null
	, token_id varchar not null
	, action varchar not null
	, trx_hash varchar not null
	, eth_value_per_token numeric not null
	, nth_trx int not null -- the nth acquisition of the same insider and collection
	, created_at date not null
	, foreign key (insider_id)  references insider(id)
	, foreign key (collection_id)  references collection(id)
);
create unique index "insight_trx_unique_idx_insider_id_collection_id_nth_trx" ON insight_trx(insider_id, collection_id, token_id, nth_trx);
create unique index "insight_trx_unique_idx_trx_hash_token_id" ON insight_trx(trx_hash, token_id);

create table insight (
	insider_id varchar not null -- eth address
	, collection_id varchar not null
	, started_at timestamp not null
	, total_eth_spent numeric not null
	, foreign key (insider_id)  references insider(id)
	, foreign key (collection_id)  references collection(id)
);
create unique index "insight_unique_idx_insider_id_collection_id" on insight(insider_id, collection_id);

-- the logic of how contracts are considered endorsedd by each circle
create table collection_to_circle_mapping (
	collection_id varchar not null
	, circle_id int not null
	, created_at date not null
	, foreign key (collection_id)  references collection(id)
	, foreign key (circle_id)  references circle(id)
);
create unique index "collection_to_circle_mapping_unique_idx_collection_circle" ON collection_to_circle_mapping(collection_id, circle_id);

create table post (
	id serial primary key
	, collection_id varchar not null
	, created_at date not null
	, foreign key (collection_id) references collection(id)
);

create table subscriber (
	id serial primary key
	, email varchar
);
create unique index "subscriber_unique_idx_email" ON subscriber(email);

create index eth_contracts_idx_timestamp on eth_contracts (timestamp desc);
create index eth_contracts_idx_is_nft on eth_contracts (is_nft);
create index eth_contracts_idx_address on eth_contracts (address);

create index eth_token_transfers_2017_idx_timestamp on eth_token_transfers_2017 (timestamp desc);
create index eth_token_transfers_2017_idx_contract on eth_token_transfers_2017 (contract desc);

create index eth_token_transfers_2018_idx_timestamp on eth_token_transfers_2018 (timestamp desc);
create index eth_token_transfers_2018_idx_contract on eth_token_transfers_2018 (contract desc);

create index eth_token_transfers_2019_idx_timestamp on eth_token_transfers_2019 (timestamp desc);
create index eth_token_transfers_2019_idx_contract on eth_token_transfers_2019 (contract desc);

create index eth_token_transfers_2020_idx_timestamp on eth_token_transfers_2020 (timestamp desc);
create index eth_token_transfers_2020_idx_contract on eth_token_transfers_2020 (contract desc);

create index eth_token_transfers_2021_idx_timestamp on eth_token_transfers_2021 (timestamp desc);
create index eth_token_transfers_2021_idx_contract on eth_token_transfers_2021 (contract desc);

create index eth_token_transfers_2022_idx_timestamp on eth_token_transfers_2022 (timestamp desc);
create index eth_token_transfers_2022_idx_contract on eth_token_transfers_2022 (contract desc);




create index nft_ownership_idx_owner on nft_ownership (owner);
create index nft_ownership_idx_contract on nft_ownership (contract);
create index nft_ownership_idx_token_id on nft_ownership (token_id);

create index eth_transactions_idx_timestamp on eth_transactions (timestamp desc);
create index eth_transactions_idx_trx_hash on eth_transactions (trx_hash desc);