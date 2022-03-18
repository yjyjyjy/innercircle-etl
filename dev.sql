

-- Trust
-- How did the insiders make profit?
-- Their recent trading performance?

-- Track
-- What do the insiders own
-- What do the insiders buy / sell?



select collection_id
, count(distinct insider_id)
, count(1)
from insight_trx
where insider_id in (
select insider_id
from insider_to_circle_mapping
	where circle_id = 2
)
and date >= date(now() - interval '7 day')
group by 1
order by 2 desc;
