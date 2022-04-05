
insert into address_metadata (id, is_special_address, special_address_type)
select
	payment_token
	, true
	, 'currency'
from (
	select
		payment_token
	from nft_trades
	where timestamp > date(now() - interval '30 days')
	group by 1
	) a
where payment_token not in (select id from address_metadata)
;


update eth_contracts
set is_nft = false
where address in (
	select
		id
	from address_metadata
	where special_address_type = 'currency'
)
;


select *
from eth_contracts
where address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' -- WETH
;


delete
from nft_trx_union
where contract in (
	select
		id
	from address_metadata
	where special_address_type = 'currency'
	group by 1
)
;

select
	action
	, count(1)
from nft_trx_union
where timestamp > '2022-01-01'
group by 1
order by 2 desc
;

select
	count(1)
	, count(distinct a.contract)
from eth_token_transfers_2022 a
join tmp
	on tmp.address = a.contract
where timestamp >= '2022-03-17'
;
-- 384704	3535

select
	count(1)
	, count(distinct a.contract)
from nft_trx_union a
join tmp
	on tmp.address = a.contract
where timestamp >= '2022-03-17'
;
-- 154807	3504

select
	count(1)
	, count(distinct a.contract)
from nft_trx_union a
where timestamp >= '2022-03-17'
;
-- 154807	3504

-- what's missing from token_transfers?
drop table if exists tmp;
create table tmp as
select address
from eth_contracts
where is_nft
;
create unique index tmp_idx on tmp (address);

drop table if exists cet_token_transfers;
create table cet_token_transfers as
select
	a.*
from eth_token_transfers_2022 a
join tmp
	on tmp.address = a.contract
where timestamp >= '2022-03-17'
;

create index cet_token_transfers_idx on cet_token_transfers (trx_hash);

drop table if exists cet_nft_trx_union;
create table cet_nft_trx_union as
select
	a.*
from nft_trx_union a
join tmp
	on tmp.address = a.contract
where timestamp >= '2022-03-17'
;


drop table if exists cet_missing;
create table cet_missing as
with transfers as (
	select
		trx_hash
	from cet_token_transfers
	group by 1
)
, trx_union as (
	select
		trx_hash
	from cet_nft_trx_union
	group by 1
)
, missing as (
	select
		f.*
	from transfers f
	left join trx_union u
		on f.trx_hash = u.trx_hash
	where u.trx_hash is null
)
select
	f.*
from cet_token_transfers f
join missing m
	on f.trx_hash = m.trx_hash
;

select
	contract
	, count(1)
from cet_missing
group by 1
order by 2 desc
;

select *
from cet_missing
limit 100;





-- 260701

select *
from cet_nft_trx_union
where trx_hash = '0xad2dec8f52a03e66f8ac127e117fa4def18df08428f5da33ccdd032979fd04c4'
;


select *
from cet_token_transfers
where trx_hash = '0xad2dec8f52a03e66f8ac127e117fa4def18df08428f5da33ccdd032979fd04c4'
 and contract = '0xb33440566865f4433e28f90f61f0a8ec334b761e'
 and token_id_or_value = '6705078124'
 and to_address = '0x479e58312dc2527b66ca89771a5c337d361d1e22'
 ;

select token_id_or_value, *
from eth_token_transfers_2022
limit 100;

select *
from nft_trx_union
limit 10;


select count(1)
from eth_contracts
where is_erc721
;

select * from nft_trx_union limit 10;


select count(1) from tmp limit 10;

select
	is_nft
	, is_erc721
	, count(1)
from eth_contracts
group by 1,2
;

select *
from eth_contracts
where
	address = lower('0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2')
;



select * from eth_token_transfers_2022 limit 10;
delete
from nft_ownership where contract in (
                        select
                            id
                        from address_metadata
                        where special_address_type = 'currency'
                        group by 1
                    );



