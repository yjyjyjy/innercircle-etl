
-- 📆 Mar 14 2022
/*
Quality check the trade data:
-- WETH good? yes
-- ETH == Calculated ETH?
-- What % of trades have Floor price == 0?
*/

select * from nft_trades limit 100;

select max(timestamp)
from nft_trades
;

-- WETH good? yes
select payment_token
, count(1)
from nft_trades
where timestamp >= '2022-03-10'
group by 1
order by 2 desc
;
/*
"0x0000000000000000000000000000000000000000"	47642
"0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"	2078
"0x64d91f12ece7362f91a6f8e7940cd55f05060b92"	195
"0x15d4c048f83bd7e37d49ea4c83a07267ec4203da"	45
"0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"	20
"0x5881da4527bcdc44a100f8ba2efc4039243d2c07"	9
"0x3845badade8e6dff049820680d1f14bd3903a5d0"	4
"0x0f5d2fb29fb7d3cfee444a200298f468908cc942"	2
"0x5c761c1a21637362374204000e383204d347064c"	1
"0x6b175474e89094c44da98b954eedeac495271d0f"	1
"0x95ad61b0a150d79219dcf64e1e6cc01f0b64c4ce"	1
"0x15f0eedf9ce24fc4b6826e590a8292ce5524a1da"	1
*/

-- 0x0000000000000000000000000000000000000000 ETH
-- 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 WETH
-- 0x64d91f12ece7362f91a6f8e7940cd55f05060b92 ASH
-- 0x15d4c048f83bd7e37d49ea4c83a07267ec4203da GALA
-- 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48 USDC
-- 0x5881da4527bcdc44a100f8ba2efc4039243d2c07 LGBTQ
-- 0x3845badade8e6dff049820680d1f14bd3903a5d0 SAND
-- 0x0f5d2fb29fb7d3cfee444a200298f468908cc942 MANA


-- ETH == Calculated ETH?
select
	eth_value = price
	, count(1)
from nft_trades
where payment_token = '0x0000000000000000000000000000000000000000'
	and timestamp between '2022-03-10' and '2022-03-11'
group by 1
;
/* GOOD
false	50
true	47592
*/

-- What % of trades have Floor price == 0?
select
	price > 0 as more_than_0
	, price = 0 as equal_0
	, price < 0 as less_than_0
	, count(1)
from nft_trades
where payment_token in (
	'0x0000000000000000000000000000000000000000' -- ETH
	, '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' -- WETH
)
	and timestamp between '2022-03-10' and '2022-03-11'
group by 1,2,3
order by 1,2,3
;
/*
false	true	false	453
true	false	false	49267
*/
-- less than 1% of the records are zero value trades. weird but fine