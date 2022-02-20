drop table if exists shadow_trade;
create table shadow_trade (
	shadow_trade_id serial primary key
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
)
;
create unique index "shadow_trade_unique_idx_insider_id_col_id_token_id_entry_ts" ON shadow_trade(insider_id, collection_id, token_id, entry_timestamp);


drop table if exists tmp_purchase;
create table tmp_purchase as
	select
		i.id as insider_id
		, t.contract as collection_id
		, t.token_id
		, t.eth_value_per_token as entry_price
		, p.floor_price_in_eth as entry_floor_price
		, t.timestamp as entry_timestamp
	from insider i
	join nft_trx_union t
		on i.id = t.to_address
	left join nft_contract_floor_price p
		on p.contract = t.contract
		and p.date = date(t.timestamp)
	where t.timestamp >= now() - interval '3 months'
		and t.action in ('trade', 'mint')
		and t.eth_value_per_token > 0
;

drop table if exists tmp_sell;
create table tmp_sell as
	select
		i.id as insider_id
		, t.contract as collection_id
		, t.token_id
		, t.eth_value_per_token as exit_price
		, p.floor_price_in_eth as exit_floor_price
		, t.timestamp as exit_timestamp
		, t.action
	from insider i
	join nft_trx_union t
		on i.id = t.from_address
	left join nft_contract_floor_price p
		on p.contract = t.contract
		and p.date = date(t.timestamp)
	where t.timestamp >= now() - interval '3 months'
		and t.action in ('trade', 'burn', 'transfer')
;

create table tmp_collection_latest_floor_price as
with cet as (
	select
		contract
		, floor_price_in_eth
		, row_number() over (partition by contract order by date desc) as rnk
	from nft_contract_floor_price
)
select
	contract
	, floor_price_in_eth
from cet
where rnk = 1
;
drop table if exists tmp_staging;
create table tmp_staging as
select
	p.insider_id
	, p.collection_id
	, p.token_id
	, p.entry_price
	, p.entry_floor_price
	, p.entry_timestamp
	, s.exit_price
	, s.exit_timestamp
	, l.floor_price_in_eth as latest_price
	, (case when s.action in ('burn', 'transfer') then null
		when s.action = 'trade' then (s.exit_price - p.entry_price)/p.entry_price
		when s.exit_price is null then (l.floor_price_in_eth - p.entry_floor_price)/p.entry_floor_price
	end) as profit_or_loss
	, s.action
	, row_number() over (partition by p.insider_id, p.collection_id, p.token_id, p.entry_timestamp order by s.exit_timestamp) as rnk
from tmp_purchase p
join tmp_collection_latest_floor_price l
	on l.contract = p.collection_id
left join tmp_sell s
	on p.insider_id = s.insider_id
	and p.collection_id = s.collection_id
	and p.token_id = s.token_id
	and s.exit_timestamp > p.entry_timestamp
;

truncate table shadow_trade;
insert into shadow_trade (
	insider_id
	, collection_id
	, collection_name
	, token_id
	, entry_price
	, entry_floor_price
	, entry_timestamp
	, exit_price
	, exit_timestamp
	, latest_price
	, profit_or_loss
)
select
	a.insider_id
	, a.collection_id
	, c.name as collection_name
	, a.token_id
	, a.entry_price
	, entry_floor_price
	, entry_timestamp
	, exit_price
	, exit_timestamp
	, latest_price
	, profit_or_loss
from tmp_staging a
left join collection c
	on a.collection_id = c.id
where (action = 'trade' or action is null)
	and profit_or_loss is not null
	and rnk = 1
	and profit_or_loss >= -1
;

drop table if exists shadow_trade_summary;
create table shadow_trade_summary (
	insider_id varchar
	, collection_id varchar
	, entry_timestamp timestamp
	, profit_or_loss numeric
	, primary key (insider_id, collection_id)
	, foreign key (insider_id)  references insider(id)
	, foreign key (collection_id)  references collection(id)
)
;

insert into shadow_trade_summary
select
	insider_id
	, collection_id
	, min(entry_timestamp) as entry_timestamp
	, avg(profit_or_loss) as profit_or_loss
from shadow_trade
group by 1,2
;
