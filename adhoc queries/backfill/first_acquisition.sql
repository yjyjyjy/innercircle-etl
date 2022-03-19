drop table if exists first_acquisition;
create table first_acquisition as
select
	timestamp
	, trx_hash
	, contract
	, token_id
	, from_address
	, to_address
	, trade_platform
	, trade_payment_token
	, num_tokens_in_the_same_transaction
	, price_per_token
	, action
	, caller_is_receiver
from (
	select
		*
		, row_number() over (partition by contract, to_address order by timestamp) as rnk
	from nft_trx_union
) a
where rnk = 1;

create unique index first_acquisition_idx_contract_to_address on first_acquisition (contract, to_address);
create index first_acquisition_idx_to_address on first_acquisition (to_address);

