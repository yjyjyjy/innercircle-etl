with cet_collections as (
	select
		contract
		, count(distinct owner) as num_collectors
	from notable_collector_recent_collections
	where owner_rank <= 200
	group by 1
)
select
	c.num_collectors
	, col.*
	, m.name
	, m.external_url
from cet_collections c
join notable_collector_recent_collections col
	on c.contract = col.contract
	and col.owner != '0xe052113bd7d7700d623414a0a4585bcae754e9d5' -- nifty gateway
left join nft_contract_metadata m
	on m.address = c.contract
where owner_rank <= 200
order by num_collectors desc, m.name, owner_rank
;

select o.*, w.owner_rank
from nft_ownership_no_transfer o
join (
	select owner, owner_rank
	from notable_collector_recent_collections
	where owner_rank <= 200
	group by 1,2
) w
	on w.owner = o.owner
where contract = '0x0f78c6eee3c89ff37fd9ef96bd685830993636f2'
order by owner_rank
;



-- most heavily traded contracts by quarter
with stats as (
	select
	date(date_trunc('quarter', timestamp)) as quarter
	, nft_contract
	, count(1) as ct
	from nft_trades
	group by 1,2
)
, cet as (
	select
		quarter
		, nft_contract
		, ct
		, row_number() over (partition by quarter order by ct desc) as rnk
	from stats
	)
select
	quarter
	, nft_contract
	, ct
	, rnk
from cet
where rnk <= 2
order by 1,3 desc
;

-- 0xa7d8d9ef8d8ce8992df33d8b8cf4aebabd5bd270 artblocks
-- 0x495f947276749ce646f68ac8c248420045cb7b5e openSea shared storefront
-- ENS '0x283af0b28c62c092c9727f1ee09c02ca627eb7f5'
-- ens reserve '0x084b1c3c81545d370f3634392de611caabff8148'
create table nft_trx_union (
	timestamp timestamp,
	trx_hash varchar,
	contract varchar,
	token_id varchar,
	from_address varchar,
	to_address varchar,
	num_tokens_in_the_same_transaction int,
	eth_value numeric,
	action varchar
)
;

insert into nft_trx_union
with nft_token_transfers as (
	select
		trans.*
	from eth_token_transfers trans
	join eth_contracts con
		on con.address = trans.contract
	where trans.timestamp >= '2017-06-01' -- punks was launched in that month
		and con.is_nft
)
, num_tokens as (
	select
		trx_hash
	, count(distinct token_id_or_value) as num_tokens_in_the_same_transaction
	from nft_token_transfers
	group by 1
)
select
	trans.timestamp
	, trans.trx_hash
	, trans.contract
	, trans.token_id_or_value as token_id
	, trans.from_address
	, trans.to_address
	, mul.num_tokens_in_the_same_transaction
	, coalesce(trades.eth_value, 0) / coalesce(mul.num_tokens_in_transaction, 1) as eth_value_per_token
	, case when to_address in (
			'0x0000000000000000000000000000000000000000'
			,'0x000000000000000000000000000000000000dead'
			) then 'burn'
		when from_address = '0x0000000000000000000000000000000000000000'
			and to_address not in (
				'0x0000000000000000000000000000000000000000'
				,'0x000000000000000000000000000000000000dead'
				) then 'mint'
		when trades.trx_hash is not null then 'trade'
		else 'transfer'
		end as action
from nft_token_transfers trans
left join nft_trades trades
	on trans.trx_hash = trades.trx_hash
left join num_tokens mul
	on trans.trx_hash = mul.trx_hash
;

insert into nft_trx_union
with trans as (
	select
		trx.timestamp
		, trx.trx_hash
		, trx.contract
		, trx.from_address
		, trx.to_address
		, trx.token_id_or_value as token_id
	from eth_token_transfers trx
	join nft_contract_metadata con
		on con.address = trx.contract
	left join new_nft_contract new
		on trx.contract = new.contract
	where (trx.timestamp > select max(timestamp) from nft_trx_union)
		or (new.contract is not null)
)
, num_tokens as (
	select
		trx_hash
	, count(distinct token_id) as num_tokens_in_transaction
	from trans
	group by 1
)
, trades as (
	select
		trx.trx_hash
		, trx.eth_value
	from eth_trades trx
	left join new_nft_contract new
		on trx.to_address = new.contract
	where (trx.timestamp > select max(timestamp) from nft_trx_union)
		or (new.contract is not null)
)
select
	trans.timestamp
	, trans.trx_hash
	, trans.contract
	, trans.token_id_or_value as token_id
	, trans.from_address
	, trans.to_address
	, mul.num_tokens_in_transaction
	, coalesce(val.eth_value, 0) / coalesce(mul.num_tokens_in_transaction, 1) as eth_value_per_token
	, case when to_address in (
			'0x0000000000000000000000000000000000000000','0x000000000000000000000000000000000000dead'
			) then 'burn'
		when from_address = '0x0000000000000000000000000000000000000000'
			and to_address not in (
				'0x0000000000000000000000000000000000000000',	'0x000000000000000000000000000000000000dead'
				) then 'mint'
		when trades.trx_hash is not null then 'trade'
		else 'transfer'
		end as action
from trans
left join trades
	on trans.trx_hash = trades.trx_hash
left join num_tokens mul
	on trans.trx_hash = mul.trx_hash
;




insert into nft_owership_staging
with cet as (
	select
		contract
		, token_id
		, to_address as owner
		, row_number() over (partition by contract, token_id order by timestamp desc) as rnk
	from nft_trx_union trx
	left join new_nft_contract new
		on trx.contract = new.contract
	where new.contract is not null
		or trx.timestamp > now() - interval '3 day'
)
select
	contract
	, token_id
	, owner
from cet
where rnk = 1
;

update nft_owership tar
set owner = sour.owner
from nft_owership_staging sour
where sour.contract = tar.contract
	and sour.token_id = tar.token_id
;

insert into nft_owership
select
	contract
	, token_id
	, owner
from nft_owership_staging sour
left join nft_owership tar
	on sour.contract = tar.contract
	and sour.token_id = tar.token_id
where tar.contract is null
;

select
	coalesce(n.contract, o.contract) as contract
, 	coalesce(n.token_id, o.token_id) as token_id
, 	coalesce(n.owner, o.owner) as owner
from new_ownership n
full outer join nft_owership o
	on n.contract = o.contract
	and n.token_id = o.token_id
;



create table new_nft_contract (
	contract varchar primary key
);

insert into new_nft_contract
select address
from nft_contract_metadata
group by 1;

----------------
-------------------
--- new sql dump

select *
from nft_contract_abi
limit 10;

select *
from eth_transactions
limit 10;

select *
from eth_token_transfers
where trx_hash = '0x930e1bdfcfc0275eb33493707c5d6c653423109cab01e03892e87f1c6e26077d'
limit 10;

select *
from nft_trx_union
where action = 'trade'
limit 100;

select *
from nft_trades
--where trx_hash = '0x930e1bdfcfc0275eb33493707c5d6c653423109cab01e03892e87f1c6e26077d'
limit 10;

select *
from


'0x000000000000000000000000000000000000dead'



select * from eth_transactions limit 10;

select *
from nft_trx_union
where date(timestamp)= '2021-12-25'
	and action = 'burn'
limit 10;

select
	date(timestamp)
	, count(1)
from nft_trx_union
group by 1
order by 1;

select min(timestamp)
from nft_trx_union
limit 10
;

select *
from nft_trx_union
where date(timestamp) = '2017-11-18'
limit 100
;

select *
from nft_trx_union
where from_address = '0x0000000000000000000000000000000000000000'
	and to_address = '0x0000000000000000000000000000000000000000'
;

with cet as (
	select *
	from nft_trx_union
	where timestamp >= cast('2021-12-12' as timestamp)
		and timestamp < cast('2021-12-13' as timestamp)
		and action = 'trade'
)
, rep as (
	select
	trx_hash
	, count(1) as ct
	from cet
	group by 1
)
, dupes as (
	select
		trx_hash
	from rep
	where ct > 1
	limit 10
)
select b.*
from dupes d
join cet b
	on d.trx_hash = b.trx_hash
order by b.trx_hash
;


with cet as (
	select *
	from nft_trades
	where timestamp >= cast('2021-12-12' as timestamp)
		and timestamp < cast('2021-12-13' as timestamp)
)
, rep as (
	select
	trx_hash
	, count(1) as ct
	from cet
	group by 1
)
, dupes as (
	select
		trx_hash
	from rep
	where ct > 1
	limit 10
)
select b.*
from dupes d
join cet b
	on d.trx_hash = b.trx_hash
order by b.trx_hash
;

select datediff(now();

select now() + (duration_in_hours * interval '1 hour')
select now() + interval '-7 day'


















drop table transfers;
create table transfers as
select
	trx.timestamp
	, trx.trx_hash
	, trx.contract
	, trx.from_address
	, trx.to_address
	, trx.token_id_or_value as token_id
from nft_contract_metadata con
join eth_token_transfers trx
	on con.address = trx.contract
left join new_nft_contract new
	on trx.contract = new.contract
;


-- where
-- 	(new.contract is not null and trx.timestamp > '2015-01-01')
-- 	or (new.contract is null and trx.timestamp > now() - interval '7 day')


select min(timestamp) from transfers;
select min(timestamp) from

				;

select count(1) from new_nft_contract; -- 8422
select count(1) from nft_contract_metadata; -- 8422



create table nft_trx_union_new as
with cet as (
	select
		trx.timestamp
		, trx.trx_hash
		, trx.contract
		, trx.from_address
		, trx.to_address
		, trx.token_id_or_value as token_id
	from nft_contract_metadata con
	join eth_token_transfers trx
		on con.address = trx.contract
	left join new_nft_contract new
		on trx.contract = new.contract
	where new.contract is not null
		or trx.timestamp > now() + interval '-7 day'
)
, num_tokens as (
	select
		trx_hash
	, count(distinct token_id) as num_tokens_in_transaction
	from cet
	group by 1
)
, transaction_eth_val as (
	select
		trx.trx_hash
		, trx.eth_value
	from eth_transactions trx
	join nft_contract_metadata con
		on trx.to_address = con.address
	left join new_nft_contract new
		on trx.to_address = new.contract
	where new.contract is not null
		or trx.timestamp > now() + interval '-7 day'
)
, trades as (
	select
		trd.trx_hash
	from nft_trades trd
	join nft_contract_metadata con
		on trd.nft_contract = con.address
	left join new_nft_contract new
		on trd.nft_contract = new.contract
	where new.contract is not null
		or trd.timestamp > now() + interval '-7 day'
	group by 1
)
select
	cet.timestamp
	, cet.trx_hash
	, cet.contract
	, cet.from_address
	, cet.to_address
	, cet.token_id
	, mul.num_tokens_in_transaction
	, coalesce(val.eth_value, 0) / coalesce(mul.num_tokens_in_transaction, 1) as eth_value_per_token
	, case when to_address = '0x0000000000000000000000000000000000000000' then 'burn'
		when from_address = '0x0000000000000000000000000000000000000000' and to_address != '0x0000000000000000000000000000000000000000' then 'mint'
		when trades.trx_hash is not null then 'trade'
		else 'transfer'
		end as action
from cet
left join trades
	on cet.trx_hash = trades.trx_hash
left join num_tokens mul
	on cet.trx_hash = mul.trx_hash
left join transaction_eth_val val
	on cet.trx_hash = val.trx_hash
;


select count(1)
from nft_trx_union
left join
;


create table nft_owership as
with cet as (
	select
		contract
		, token_id
		, to_address as owner
		, row_number() over (partition by contract, token_id order by timestamp desc) as rnk
	from nft_trx_union trx
	left join new_nft_contract new
		on trx.contract = new.contract
	where new.contract is not null
		or trx.timestamp > now() + interval '-7 day'
)
, new_owership as (
	select
		contract
		, token_id
		, owner
	from cet
)
;



create table new_nft_contract (
	contract varchar primary key
);

insert into new_nft_contract
select address
from nft_contract_metadata
group by 1;