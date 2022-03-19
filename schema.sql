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
	, action varchar --transfer (137249), mint (121408), trade (68903), burn (4431)
	, caller_is_receiver BOOLEAN -- meaning the wallet received the token initiated the call
)
;
create index nft_trx_union_idx_timestamp on nft_trx_union (timestamp desc);
create index nft_trx_union_idx_contract on nft_trx_union (contract);
-- create index nft_trx_union_idx_contract_token_id on nft_trx_union (contract, token_id desc);
-- create index nft_trx_union_idx_from_address on nft_trx_union (from_address);
-- create index nft_trx_union_idx_to_address on nft_trx_union (to_address);
-- create index nft_trx_union_idx_action on nft_trx_union (action);

create table nft_ownership (
	contract varchar,
	token_id varchar,
	address varchar,
	last_transferred_at timestamp,
	PRIMARY KEY(contract, token_id)
);
create index nft_ownership_idx_address on nft_ownership (address);

-- backfill nft_ownership
drop table if exists nft_ownership;
create table nft_ownership as
with cet as (
	select
		contract
		, token_id
		, to_address
		, timestamp
		, row_number() over (partition by contract, token_id order by timestamp desc) as rnk
	from nft_trx_union
)
select
	contract
	, token_id
	, to_address as address
	, timestamp as last_transferred_at
from cet
where rnk = 1
;
create index nft_ownership_idx_address on nft_ownership (address);
ALTER TABLE nft_ownership ADD PRIMARY KEY (contract, token_id);

create table nft_contract_floor_price (
	date date,
	contract varchar,
	floor_price_in_eth numeric,
	primary key (date, contract)
)
;

create table past_90_days_trading_roi (
	address varchar(100) not null
	, contract varchar(100) not null
	, gain numeric not null
	, collection_gain_rank_in_portfolio int not null
	, total_gain numeric not null
);
create past_90_days_trading_roi_idx_address on past_90_days_trading_roi (address);

-- mapping logic from insider to circle
create table insider_to_circle_mapping (
	insider_id varchar not null
	, owner_rank int
	, circle_id int not null
	, created_at date not null
	, is_current BOOLEAN not null
	, foreign key (insider_id)  references insider(id)
	, foreign key (circle_id)  references circle(id)
);
create unique index insider_to_circle_mapping_unique_idx_insider_id_circle_id_created_at ON insider_to_circle_mapping(insider_id, circle_id, created_at);
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
	, special_address_type varchar
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

create table insider_portfolio (
	insider_id varchar not null
	, address_rank int
	, collection_id varchar not null
	, num_tokens int not null
	, floor_price_in_eth numeric
	, collection_worth numeric
	, collection_rank_in_portfolio int
	, total_worth numeric
	, collection_pct_total numeric
	, foreign key (insider_id)  references insider(id)
	, foreign key (collection_id)  references collection(id)
)
;
create unique index insider_portfolio_idx_unique on insider_portfolio (insider_id, collection_id, num_tokens);


-- get all collection bought or mint by the insiders and the first date for each collection/insider pair
create table insight_trx (
	date date  not null
	, insider_id varchar(100) not null
	, action varchar(100) not null
	, collection_id varchar(100)  not null
	, floor_price_in_eth numeric
	, num_tokens int not null
	, total_eth_amount numeric not null
	, foreign key (insider_id)  references insider(id)
	, foreign key (collection_id)  references collection(id)
)
;
create unique index insight_trx_unique_idx
	on insight_trx (
		date
		, insider_id
		, action
		, collection_id
	)
;

create table insight (
	insider_id varchar not null -- eth address
	, collection_id varchar not null
	, action varchar not null
	, num_tokens int not null
	, total_eth_amount numeric not null
	, last_traded_at timestamp not null
	, num_tokens_owned int not null
	, insight_time_decay numeric not null
	, past_90_days_trading_gain numeric not null
	, pct_trades_profitable numeric not null
	, circle_collection_first_ts timestamp not null
	, insider_collection_first_ts timestamp not null
	, circle_collection_first_time_decay numeric not null
	, insider_collection_first_time_decay numeric not null
	, feed_importance_score numeric not null
	, foreign key (insider_id)  references insider(id)
	, foreign key (collection_id)  references collection(id)
);
create unique index "insight_unique_idx_insider_id_collection_id_action"
	on insight(insider_id, collection_id, action);


-- the logic of how contracts are considered endorsedd by each circle
create table collection_to_circle_mapping (
	collection_id varchar not null
	, circle_id int not null
	, started_at date not null
	, foreign key (collection_id)  references collection(id)
	, foreign key (circle_id)  references circle(id)
);
create unique index "collection_to_circle_mapping_unique_idx_collection_circle" ON collection_to_circle_mapping(collection_id, circle_id);

create table post (
	id serial primary key
	, collection_id varchar not null
	, created_at date not null
	, feed_importance_score numeric not null
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

create index eth_transactions_idx_timestamp on eth_transactions (timestamp desc);
create index eth_transactions_idx_trx_hash on eth_transactions (trx_hash desc);