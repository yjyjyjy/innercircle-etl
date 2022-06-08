insert into address_social ( address, platform, username)
with source as (
	select
		id
		, twitter_username
	from address_metadata
	where twitter_username is not null
)
select
	s.id
	, 'twitter'
	, s.twitter_username
from source s
left join address_social t
	on s.id = t.address
	and s.twitter_username = t.username
	and t.platform = 'twitter'
where t.address is null
;

insert into address_social ( address, platform, username)
with source as (
	select
		id
		, instagram_username
	from address_metadata
	where instagram_username is not null
)
select
	s.id
	, 'instagram'
	, s.instagram_username
from source s
left join address_social t
	on s.id = t.address
	and s.instagram_username = t.username
	and t.platform = 'instagram'
where t.address is null
;

update address_social a
	set followers =
		cast (
			case
			when lower(s.followers) like '%k'
				then cast(replace(lower(s.followers),'k', '') as numeric)*1000
			when lower(s.followers) like '%m'
				then cast(replace(lower(s.followers),'m', '') as numeric)*1000000
			else cast(replace(lower(s.followers), ',', '') as numeric)
			end
			as int
		)
	, real_account = s.verified
	, last_verified_at = case when s.last_verified_at = true then 'real account only' else 'false' end
from upload_twitter_profile s
where a.username = s.twitter_username
	and platform = 'twitter'
;

create table export_wallstreetfam as
select
	a.*
	, s.real_account
	, s.followers
	, w.total_worth
	, c.name as top_collection_name
	, w.collection_worth as top_collection_worth
	, w.num_tokens as top_collection_num_tokens
	, w.collection_pct_total as top_collection_worth_as_pct_total_worth
	, w.contract as top_collection_contract_address
from wallstreetfam a
left join address_social s
	on a.address = s.address
	and a.twitter = s.username
	and s.platform = 'twitter'
	and s.followers is not null
left join address_collection_total_worth w
	on w.address = a.address
	and w.rnk = 1
left join collection c
	on c.id = w.contract
;
