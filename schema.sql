/*
PART I:
schemas that for the production website
*/


-- drop table user_to_user_profile_mapping;
-- create table user_to_user_profile_mapping (
-- 	user_id varchar not null
-- 	, user_profile_id int not null
-- 	, foreign key (user_id) references "User"(id)
-- 	, foreign key (user_profile_id) references user_profile(id)
-- );
-- create unique index user_to_user_profile_mapping_idx_user_id_user_profile_id on user_to_user_profile_mapping (user_id, user_profile_id);

drop table user_profile;
create table user_profile (
	id serial primary key
	, handle varchar not null
	, profile_name varchar not null
	, profile_picture varchar
	, email varchar not null
	, twitter varchar
	, linkedin varchar
	, bio_short varchar
	, bio varchar
-- labels
	, label_hiring bool
	, label_open_to_work bool
	, label_open_to_cofounder_matching bool
	, label_need_product_feedback bool
	, label_open_to_discover_new_project bool
	, label_fundraising bool
	, label_open_to_invest bool
	, label_on_core_team bool
	, label_text_hiring varchar
	, label_text_open_to_work varchar
	, label_text_open_to_discover_new_project varchar
-- 	building skills
	, skill_founder bool
	, skill_web3_domain_expert bool
	, skill_artist bool
	, skill_frontend_eng bool
	, skill_backend_eng bool
	, skill_fullstack_eng bool
	, skill_blockchain_eng bool
	, skill_data_eng bool
	, skill_data_science bool
	, skill_hareware_dev bool
	, skill_game_dev bool
	, skill_dev_ops bool
	, skill_product_manager bool
	, skill_product_designer bool
	, skill_token_designer bool
	, skill_technical_writer bool
	-- Growth Skills
	, skill_social_media_influencer bool
	, skill_i_bring_capital bool
	, skill_community_manager bool
	, skill_marketing_growth bool
	, skill_business_development bool
	, skill_developer_relations bool
	, skill_influencer_relations bool
	, skill_investor_relations bool
	, resume varchar
	, foreign key (email) references "User"(email)
);
CREATE UNIQUE INDEX user_profile_unique_idx_handle ON user_profile (handle);
CREATE UNIQUE INDEX user_profile_unique_idx_email ON user_profile (email);
-- join on email.
-- each email is a new identity.

select * from "User" limit 10;
select * from user_profile limit 10;
truncate table user_profile ;

create table conference (
	id serial primary key
	, year int not null
	, conference_name varchar not null
	, location varchar
	, start_date date
	, end_date date
	, website varchar
);
create unique index conference_idx_conference_name_start_date on conference (conference_name, start_date);

create table user_profile_to_conference_mapping (
	user_profile_id int not null
	, conference_id int not null
	, foreign key (user_profile_id) references user_profile(id)
	, foreign key (conference_id) references conference(id)
);
create unique index user_profile_to_conference_mapping_idx_user_profile_id_conference_id on user_profile_to_conference_mapping (user_profile_id, conference_id);


insert into conference (year, conference_name, start_date, end_date)
values
(2022, 'Consensus', '2022-06-09', '2022-06-12')
, (2022, 'ETH New York', '2022-06-24', '2022-06-26')
, (2022, 'NFT.NYC', '2022-06-20', '2022-06-23')
, (2022, 'ETH Barcelona', '2022-07-06', '2022-07-08')
;

-- insert into user_profile (
-- 	handle
-- 	, profile_name
-- 	, email
-- 	, profile_picture
-- )
-- values (
-- 	'darshan'
-- 	, 'darshan'
-- 	, 'darshanraju9@gmail.com'
-- 	, 'https://en.gravatar.com/userimage/67165895/bd41f3f601291d2f313b1d8eec9f8a4d.jpg?size=200'
-- )
-- ;

-- insert into user_profile
-- values (
-- 	1
-- 	, 'ethtomato'
-- 	, 'Junyu'
-- 	, 'prowessyang@gmail.com'
-- 	, 'ethtomato'
-- 	, 'yang-jun-yu'
-- 	, 'Co-founder @ innerCircle.ooo
-- Building the people connector for web3.
-- Finding other web3 builders is hard. Twitter is dominanted by a few influencer. Discord is so noisy and scammy. LinkedIn is filled with Web2 people. Time to build our own web3 presence, in a web3 way.
-- Join us at innerCircle.ooo'
-- 	, 'early users'
-- 	, 'data analytics'
-- 	, true
-- 	, false
-- 	, true
-- 	, false
-- 	, 'https://en.gravatar.com/userimage/67165895/bd41f3f601291d2f313b1d8eec9f8a4d.jpg?size=200'
-- 	, null
-- )
-- ;


-- insert into user_profile
-- values (
-- 	2
-- 	, '22222'
-- 	, 'Junyu22'
-- 	, 'prowessyang2@gmail.com'
-- 	, 'ethtomato2'
-- 	, 'yang-jun-yu'
-- 	, 'Co-founder2222'
-- 	, 'early users'
-- 	, 'data analytics'
-- 	, true
-- 	, false
-- 	, true
-- 	, false
-- 	, 'https://en.gravatar.com/userimage/67165895/bd41f3f601291d2f313b1d8eec9f8a4d.jpg?size=200'
-- 	, null
-- )
-- ;

select * from "User" where id = 'cl3yotf7m0006m04ls517vfrl';

select * from user limit 100;

select * from "Session" limit 10;
select *
from "Account"
where "providerAccountId" = '111140146197096860395'
limit 10;

select * from "User" limit 10;
select * from user_ limit 10;

COPY user_profile(
	profile_name,
	handle,
	email,
	bio_short,
	label_on_core_team,
	label_open_to_work,
	label_open_to_invest,
	label_open_to_discover_new_project,
	label_hiring,
	label_fundraising,
	label_need_product_feedback,
	label_text_open_to_work,
	label_text_open_to_discover_new_project,
	skill_product_designer,
	skill_fullstack_eng,
	skill_backend_eng,
	skill_artist,
	skill_product_manager,
	skill_marketing_growth,
	skill_community_manager,
	skill_data_science,
	skill_business_development,
	skill_technical_writer,
	skill_social_media_influencer,
	skill_web3_domain_expert,
	skill_founder,
	skill_i_bring_capital,
	skill_hareware_dev,
	linkedin,
	twitter )
FROM '/home/junyuyang/csv/responses_upload.csv'
DELIMITER ','
CSV HEADER;


ALTER TABLE user_profile DISABLE TRIGGER ALL;
ALTER TABLE user_profile ENABLE TRIGGER ALL;



create table connection_request (
	initiator_id int not null
	, requested_id int not null
	, created_at timestamp not null
	, confirmed_at timestamp
	, rejected_at timestamp
	, foreign key (initiator_id) references user_profile(id)
	, foreign key (requested_id) references user_profile(id)
);
create unique index connection_request_idx_initiator_id_requested_id on connection_request(initiator_id, requested_id);

create table connection (
	user_profile_start int primary key
	, user_profile_end int not null
	, created_at timestamp not null
	, foreign key (user_profile_start) references user_profile(id)
	, foreign key (user_profile_end) references user_profile(id)
);
create unique index connection_idx_user_profiles on connection(user_profile_start, user_profile_end);


/*
PART II:
Schema for the analytical server
*/


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
        slug varchar,
		last_updated_at timestamp
);

create table collection_tag (
	collection_id varchar(100)
	, tag varchar(500)
	, foreign key (collection_id)  references collection(id)
)
;
CREATE UNIQUE INDEX collection_tag_unique_idx_collection_id_tag ON collection_tag (collection_id, tag);

create table collection_similarity (
	collection_id varchar(100)
	, counterpart_collection_id varchar(100)
	, similarity numeric
	, foreign key (collection_id) references collection(id)
);
create unique index collection_similarity_unique_idx_collection_id_counterpart_collection_id on collection_similarity (collection_id, counterpart_collection_id);
create index collection_similarity_idx_collection_id on collection_similarity (collection_id);


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
	, buy_date date not null
	, buy_eth_amount numeric not null
	, gain numeric not null
	, roi_pct numeric not null
	, collection_gain_rank_in_portfolio int not null
	, total_gain numeric not null
);
create index past_90_days_trading_roi_idx_address on past_90_days_trading_roi (address);
create unique index past_90_days_trading_roi_idx_address_contract on past_90_days_trading_roi (address, contract);

create table insider_past_90_days_trading_roi (
	insider_id varchar(100) not null
	, collection_id varchar(100) not null
	, buy_date date not null
	, buy_eth_amount numeric not null
	, gain numeric not null
	, roi_pct numeric not null
	, collection_gain_rank_in_portfolio int not null
	, total_gain numeric not null
	, foreign key (insider_id)  references insider(id)
	, foreign key (collection_id)  references collection(id)
);
create index insider_past_90_days_trading_roi_idx_insider_id on insider_past_90_days_trading_roi (insider_id);
create unique index insider_past_90_days_trading_roi_idx_address_contract on insider_past_90_days_trading_roi (insider_id, collection_id);


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
	, public_name_tag varchar
	, opensea_display_name varchar
	, opensea_image_url varchar
	, opensea_banner_image_url varchar
	, opensea_bio varchar
	, ens varchar
	, twitter_username varchar
	, instagram_username varchar
	, medium_username varchar
	, email varchar
	, website varchar
	, opensea_user_created_at timestamp
	, last_updated_at timestamp
);

create table address_metadata (
	id varchar primary key
	, public_name_tag varchar
	, is_contract BOOLEAN
	, is_special_address boolean
	, special_address_type varchar
	, opensea_display_name varchar
	, opensea_image_url varchar
	, opensea_banner_image_url varchar
	, opensea_bio varchar
	, ens varchar
	, twitter_username varchar
	, instagram_username varchar
	, medium_username varchar
	, email varchar
	, website varchar
	, opensea_user_created_at timestamp
	, last_updated_at timestamp
	, twitter_username_verifed boolean
	, twitter_follower varchar
	, discord_username varchar
)
;
create index address_metadata_idx_email on address_metadata (email);
create index address_metadata_idx_is_contract on address_metadata (is_contract);
create index address_metadata_idx_is_special_address on address_metadata (is_special_address);

create table upload_twitter_profile (
	twitter_username varchar
	, verified boolean
	, followers varchar
	, last_verified_at timestamp
);
create unique index upload_twitter_profile_idx_twitter_username on upload_twitter_profile (twitter_username);

create table address_social (
	address varchar(100)
	, platform varchar(100)
	, username varchar
	, followers int
	, verified_linkage boolean -- verfied linkage between social accounts to wallets
	, real_account boolean -- real account or not. Null means unverified where not means it's not a functional account
	, last_verified_at timestamp
);
create unique index address_social_unqiue_idx on address_social (address, platform, username);


-- the opensea loading table for the address_metadata
create table address_metadata_opensea (
	id varchar primary key
	, opensea_display_name varchar
	, opensea_image_url varchar
	, opensea_banner_image_url varchar
	, opensea_bio varchar
	, twitter_username varchar
	, instagram_username varchar
	, website varchar
	, opensea_user_created_at timestamp
	, last_updated_at timestamp
)
;

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

create table insider_collection_ownership (
	insider_id varchar(100)
	, collection_id varchar(100)
	, num_tokens int
	, oldest_token_collected_at timestamp
	, newest_token_collected_at timestamp
	, num_token_buy int
	, num_token_sell int
	, net_num_token_buy int
	, foreign key (collection_id)  references collection(id)
	, foreign key (insider_id)  references insider(id)
	, primary key (insider_id, collection_id)
)
;


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