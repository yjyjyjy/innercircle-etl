//drop table COLLECTION;
//drop table ETH_CONTRACTS;
//drop table ETH_TOKEN_TRANSFERS;
//drop table ETH_TOKEN_TRANSFERS_2017;
//drop table ETH_TOKEN_TRANSFERS_2018;
//drop table ETH_TOKEN_TRANSFERS_2019;
//drop table ETH_TOKEN_TRANSFERS_2020;
//drop table ETH_TOKEN_TRANSFERS_2021;
//drop table ETH_TOKEN_TRANSFERS_2022;
//drop table ETH_TRANSACTIONS;
//drop table NEW_NFT_CONTRACTS;
//drop table NFT_CONTRACT_FLOOR_PRICE;
//drop table NFT_OWNERSHIP;
//drop table NFT_TRADES;
//DROP TABLE NFT_TRX_UNION;
//DROP TABLE OWNER_COLLECTION_TOTAL_WORTH;
//DROP TABLE OWNER_COLLECTION_WORTH;
//drop table INSIDER;
//drop table circle;

#use role explorer_role (you may use sysadmin or accountadmin)

use warehouse COMPUTE_WH
use database ANALYTICS
use schema PUBLIC

create table eth_contracts (
	address	varchar primary key
	, is_erc20 BOOLEAN
	, is_erc721 BOOLEAN
	, is_nft BOOLEAN
	, timestamp TIMESTAMP
);

create table eth_transactions (
    timestamp timestamp
    , trx_hash varchar(100) primary key
    , from_address varchar(100)
    , to_address varchar(100)
    , eth_value numeric
);

create table eth_token_transfers (
	timestamp timestamp
	, trx_hash varchar
	, contract varchar
	, from_address varchar
	, to_address varchar
	, token_id_or_value varchar
);

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


create table nft_trades (
	timestamp timestamp
	, trx_hash varchar
	, eth_value numeric
	, payment_token varchar
	, price numeric
	, platform varchar(50)
);

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

create table nft_trx_union (
	timestamp timestamp,
	trx_hash varchar,
	contract varchar,
	token_id varchar,
	from_address varchar,
	to_address varchar,
	num_tokens_in_the_same_transaction int,
	eth_value_per_token numeric,
	action varchar,
	caller_is_receiver BOOLEAN
)
;

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

drop table if exists owner_collection_total_worth;
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
drop table if exists circle;
create table circle (
	id INT PRIMARY KEY
	, name varchar
);
drop table if exists insider;
create table insider (
	id VARCHAR PRIMARY KEY
	, ens varchar
	, twitter_username varchar
	, instagram_username varchar
);

drop table if exists shadow_trade;
create table shadow_trade (
	shadow_trade_id varchar primary key
	, insider_id varchar not null
	, collection_id varchar not null
	, collection_name varchar
	, token_id varchar not null
	, entry_price numeric not null
	, entry_floor_price numeric
	, entry_timestamp timestamp not null
	, exit_price numeric
	, exit_timestamp timestamp
	, latest_price numeric not null
	, profit_or_loss numeric not null -- profit or loss
	, foreign key (insider_id) references insider(id)
	, foreign key (collection_id) references collection(id)
);

drop table if exists shadow_trade_summary;
create table shadow_trade_summary (
	insider_id varchar
	, collection_id varchar
	, entry_timestamp timestamp
	, profit_or_loss numeric
	, primary key (insider_id, collection_id)
	, foreign key (insider_id) references insider(id)
	, foreign key (collection_id) references collection(id)
);

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
    , foreign key (insider_id) references insider(id)
	, foreign key (collection_id) references collection(id)
);

create table insight (
	insider_id varchar not null -- eth address
	, collection_id varchar not null
	, started_at timestamp not null
	, total_eth_spent numeric not null
	, foreign key (insider_id) references insider(id)
	, foreign key (collection_id) references collection(id)
);

create table collection_to_circle_mapping (
	collection_id varchar not null
	, circle_id int not null
	, created_at date not null
	, foreign key (collection_id)  references collection(id)
	, foreign key (circle_id)  references circle(id)
);

create table post (
	id VARCHAR primary key
	, collection_id varchar not null
	, created_at date not null
	, foreign key (collection_id) references collection(id)
);

create table subscriber (
	id INT primary key
	, email varchar
);
