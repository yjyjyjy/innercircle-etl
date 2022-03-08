import pandas as pd
import etl_utls as utl
from const import OPENSEA_TRADING_CONTRACT
import time


def update_eth_transactions(date):
    # ******* dump raw transaction data into postgres sql *******
    print("🦄🦄 start update_eth_transactions: " + date)

    sql = f"""
        select
        block_timestamp as `timestamp`
        , trx.`hash` as trx_hash
        , from_address
        , to_address
        , value/pow(10,18) as eth_value
        from `bigquery-public-data.crypto_ethereum.transactions` trx
        where date(block_timestamp) = date('{date}')
            and receipt_status = 1
    """

    table = "eth_transactions"

    utl.copy_from_google_bigquery_to_postgres(
        sql=sql,
        table=table,
        use_upsert=False,
    )


def update_token_transfers(date, running_in_cloud=utl.RUNNING_IN_CLOUD, use_upsert=True):
    print("🦄🦄 start update_token_transfers: " + date)

    sql = f"""
    SELECT
        block_timestamp as `timestamp`
        , transaction_hash as trx_hash
        , token_address as contract
        , from_address
        , to_address
        , value as token_id_or_value
    FROM `bigquery-public-data.crypto_ethereum.token_transfers`
    WHERE DATE(block_timestamp) = '{date}'
    """
    year = date[:4]

    table = f"eth_token_transfers_{year}"

    if not running_in_cloud:
        csv_filename_with_path = utl.CSV_WAREHOUSE_PATH + utl.PATHS["eth_token_transfers"] + date + ".csv"
    else:
        csv_filename_with_path = None

    utl.copy_from_google_bigquery_to_postgres(
        sql=sql,
        table=table,
        csv_filename_with_path=csv_filename_with_path,
        use_upsert=use_upsert,
        key="trx_hash",
    )


# Decode the trades transactions from opensea each day.
# Schema: ["timestamp", "transaction_hash", "eth_value", "nft_contract", "token_id", "buyer", "seller", "platform"]
def update_nft_trade_opensea(date, running_in_cloud=utl.RUNNING_IN_CLOUD, use_upsert=True):
    print("🦄🦄 processing nft_trades opensea for " + date)

    sql = f"""
        select
            block_timestamp as `timestamp`
            , trx.`hash` as trx_hash
            , value/pow(10,18) as eth_value
            , input as input_data
        from `bigquery-public-data.crypto_ethereum.transactions` trx
        where date(block_timestamp) = date('{date}')
            and to_address='{OPENSEA_TRADING_CONTRACT}'
            and receipt_status = 1
    """

    df = utl.download_from_google_bigquery(sql=sql)
    contract = utl.get_opensea_contract()
    print("decoding opensea contract")
    df["decoded"] = df.input_data.apply(lambda x: utl.decode_opensea_trade(x, contract=contract))
    df["nft_contract"] = df.decoded.apply(lambda x: x["nft_contract"] if x is not None else None)
    df["token_id"] = df.decoded.apply(lambda x: x["token_id"] if x is not None else None)
    df["buyer"] = df.decoded.apply(lambda x: x["buyer"] if x is not None else None)
    df["seller"] = df.decoded.apply(lambda x: x["seller"] if x is not None else None)
    df["timestamp"] = df.timestamp
    df["platform"] = "opensea"
    df = df[["timestamp", "trx_hash", "eth_value", "nft_contract", "token_id", "buyer", "seller", "platform"]]

    if not running_in_cloud:
        csv_filename_with_path = utl.CSV_WAREHOUSE_PATH + utl.PATHS["nft_trades_staging_opensea"] + date + ".csv"
    else:
        csv_filename_with_path = None

    utl.copy_from_df_to_postgres(
        df=df,
        table="nft_trades",
        csv_filename_with_path=csv_filename_with_path,
        use_upsert=use_upsert,
        key="trx_hash",
    )


def update_contracts(use_upsert=True):
    print("🦄🦄 start update_contracts")

    # get max(timestamp of existing table)
    max_ts = utl.get_terminal_ts(table="eth_contracts", end="max")
    if max_ts == None:
        max_ts = "2015-01-01"

    sql = f"""
    with cet as (
        select
            address
            , is_erc20
            , is_erc721
            , is_erc721 as is_nft
            , block_timestamp as `timestamp`
            , row_number() over (partition by address order by block_timestamp desc)  as rnk
        from `bigquery-public-data.crypto_ethereum.contracts`
        where block_timestamp >= '{max_ts}'
    )
    select
        address
        , is_erc20
        , is_erc721
        , is_nft
        , `timestamp`
    from cet
    where rnk = 1
    """
    utl.copy_from_google_bigquery_to_postgres(
        sql=sql,
        table="eth_contracts",
        use_upsert=use_upsert,
        key="address",
    )


def update_contract_is_nft():
    sql = """
        update eth_contracts as con
        set is_nft = true
        FROM (
                select
                    nft_contract as address
                from nft_trades
                group by 1
            ) AS tr
        WHERE con.address = tr.address
            and con.is_nft = false
    """
    utl.query_postgres(sql)


def mark_new_contracts():
    sql = """
    delete from collection where name is null;
    truncate table new_nft_contracts;
    insert into new_nft_contracts
    with nft_trx_contract as (
        select contract
        from nft_trx_union
        where timestamp >= '2021-01-01'
        group by 1
    )
    select
        sot.address
        , meta.id is null as missing_metadata
        , nft.contract is null as missing_trx_union
    from eth_contracts sot
    left join collection meta
        on sot.address = meta.id
    left join nft_trx_contract nft
        on sot.address = nft.contract
    where sot.is_nft
        and (meta.id is null or nft.contract is null)
        and sot.timestamp >= '2021-01-01'
    ;
    """
    utl.query_postgres(sql)


# update the nft contract meta data by calling the OpenSea single asset api https://docs.opensea.io/reference/retrieving-a-single-asset
def update_collection(pagination=5):
    result = utl.query_postgres(sql="select address from new_nft_contracts where missing_metadata", columns=["address"])
    todo_list = result.address.to_list()

    # output schema
    """
        "address",
        "name",
        "safelist_request_status",
        "description",
        "image_url",
        "banner_image_url",
        "external_url",
        "twitter_username",
        "discord_url",
        "telegram_url",
        "instagram_username",
        "medium_username",
        "wiki_url",
        "payout_address",
        "slug"
    """

    wait_time = 1.5
    while len(todo_list) > 0:
        print(f"🦾🦾 todo list len : {len(todo_list)}")
        _todo = todo_list[:pagination]
        output = pd.DataFrame()

        for contract in _todo:
            try:
                meta, status_code = utl.get_contract_meta_data_from_opensea(contract)
            except Exception as e:
                print("🤯 Error decoding contract meta data", e)
                if status_code != None:
                    print("status_code = " + status_code)
                continue

            if status_code in [429, 404]:
                print(f"⏱ current wait_time: {wait_time}")
                time.sleep(60)
                if wait_time <= 5:
                    wait_time += 0.5

            row = pd.DataFrame(meta, index=[0])

            if output.empty:
                output = row
            else:
                output = output.append(row)
            time.sleep(wait_time)

        print("🧪🧪🧪 upserting output data")
        print(output[["address", "name"]])

        if output.shape[0] > 0:
            utl.copy_from_df_to_postgres(df=output, table="collection", use_upsert=True, key="id")
        todo_list = [x for x in todo_list if x not in _todo]


def update_nft_contract_abi(pagination=5):
    """
    output_schema = ["address", "abi"]
    """

    result = utl.query_postgres(sql="select address from new_nft_contracts where missing_abi", columns=["address"])
    todo_list = result.address.to_list()

    while len(todo_list) > 0:
        print("🦾🦾 todo list len")
        print(len(todo_list))

        _todo = todo_list[:pagination]
        output = pd.DataFrame()
        for address in _todo:
            print(address)
            abi = utl.fetch_abi(address)
            if abi == None:
                continue

            # create a single row dataframe
            row = pd.DataFrame({"address": address, "abi": abi}, index=[0])

            if output.empty:
                output = row
            else:
                output = output.append(row)

        print("🧪🧪🧪 upserting output data")
        print(output)

        if output.shape[0] > 0:
            utl.copy_from_df_to_postgres(df=output, table="nft_contract_abi", use_upsert=True, key="address")
        todo_list = [x for x in todo_list if x not in _todo]


def backup_meta_data():
    utl.export_postgres(table="collection", csv_filename_with_path=utl.CSV_WAREHOUSE_PATH + "collection.csv")
    utl.export_postgres(table="eth_contracts", csv_filename_with_path=utl.CSV_WAREHOUSE_PATH + "eth_contracts.csv")


def update_nft_trx_union(year=None, use_upsert=True):
    print("🦄🦄 start update_nft_trx_union ")
    start_date = "2017-01-01"
    if year == None:
        max_ts = utl.get_terminal_ts(table="nft_trx_union", end="max", offset="-7 days")
        if max_ts > start_date:
            start_date = max_ts
        year = start_date[:4]
    else:
        start_date = year + "-01-01"
    sql = f"""
        with cet_nft_token_transfers as (
            select
                trans.*
                , trx.eth_value
                , trx.from_address=trans.to_address as caller_is_receiver
                , trx.to_address='0x7be8076f4ea4a4ad08075c2508e481d6c946d12b' as to_address_is_opensea
            from eth_token_transfers_{year} trans
            join eth_contracts con
                on con.address = trans.contract
            join eth_transactions trx
                on trx.trx_hash = trans.trx_hash
            left join new_nft_contracts new
                on trans.contract = new.address
                and new.missing_trx_union
            where con.is_nft
                and (
                    trx.timestamp >= '{start_date}'
                    or new.address is not null
                    )
        )
        , num_tokens as (
            select
                trx_hash
            , count(distinct token_id_or_value) as num_tokens_in_the_same_transaction
            from cet_nft_token_transfers
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
            , eth_value/mul.num_tokens_in_the_same_transaction as eth_value_per_token -- there shouldn't be div by zero
            , case when to_address in (
                    '0x0000000000000000000000000000000000000000'
                    ,'0x000000000000000000000000000000000000dead'
                    ) then 'burn'
                when from_address = '0x0000000000000000000000000000000000000000'
                    and to_address not in (
                        '0x0000000000000000000000000000000000000000'
                        ,'0x000000000000000000000000000000000000dead'
                        )
                    and caller_is_receiver
                        then 'mint'
                when to_address_is_opensea then 'trade'
                else 'transfer'
                end as action
            , caller_is_receiver
        from cet_nft_token_transfers trans
        left join num_tokens mul
            on trans.trx_hash = mul.trx_hash
        ;
    """
    df = utl.query_postgres(
        sql,
        columns=[
            "timestamp",
            "trx_hash",
            "contract",
            "token_id",
            "from_address",
            "to_address",
            "num_tokens_in_the_same_transaction",
            "eth_value_per_token",
            "action",
            "caller_is_receiver",
        ],
    )
    utl.copy_from_df_to_postgres(df=df, table="nft_trx_union", use_upsert=use_upsert, key="trx_hash")


# The EOD ownership of all the tokens based on token transfer data
# Schema ["contract", "token_id", "owner"]
def update_nft_ownership():
    print("🦄🦄 update_nft_ownership")
    sql = f"""
    drop table if exists nft_ownership;
    create table nft_ownership as
    with cet as (
        select
            contract
            , token_id
            , to_address
            , row_number() over (partition by contract, token_id order by timestamp desc) as rnk
        from nft_trx_union
    )
    select
        contract
        , token_id
        , to_address as owner
    from cet
    where rnk = 1
    ;
    create index nft_ownership_idx_owner on nft_ownership (owner);
    create index nft_ownership_idx_contract on nft_ownership (contract);
    create index nft_ownership_idx_token_id on nft_ownership (token_id);
    ;"""
    utl.query_postgres(sql)


def update_nft_contract_floor_price(date):
    print("🦄🦄 update_nft_contract_floor_price: " + date)
    sql = f"""
        insert into nft_contract_floor_price
        select
            cast('{date}' as date)
            , contract
            , percentile_disc(0.1) within group (order by eth_value_per_token)
        from nft_trx_union
        where timestamp >= date('{date}')
            and timestamp < date('{date}') + interval'1 days'
            and action = 'trade'
        group by 1,2
        ;
        --  having count(distinct trx_hash) >= 2  -- having more than 2 trades
    """
    utl.query_postgres(sql)


def update_owner_collection_total_worth():  # each owner's worth by each contract they own. Plus their total worth
    max_pricing_available_date = utl.get_terminal_ts(
        table="nft_contract_floor_price", end="max", offset=None, key="date"
    )
    sql = f"""
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
            where p.date = '{max_pricing_available_date}'
                and o.owner not in (
                    '0x0000000000000000000000000000000000000000'
                    ,'0x000000000000000000000000000000000000dead'
                    , '0x0000000000000000000000000000000000000001'
                    , '0xe052113bd7d7700d623414a0a4585bcae754e9d5' -- nifty-gateway-omnibus
                    )
                and o.contract not in (
                    '0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85' -- ENS
                    )
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
        , owner_worth as (
            select
                owner
                , sum(collection_worth) as total_worth
            from owner_collection_worth
            group by 1
        )
        , owner_worth_ranked as (
            select
                owner
                , total_worth
                , RANK () OVER (ORDER BY total_worth desc) owner_rank
            from owner_worth
        )
        select
            ocwr.owner
            , owner_rank
            , ocwr.contract
            , ocwr.collection_worth
            , rnk
            , total_worth
            , case when total_worth=0 then null else coalesce(ocwr.collection_worth,0) / total_worth end as collection_pct_total
        from owner_collection_worth_ranked ocwr
        join owner_worth_ranked owr
            on ocwr.owner = owr.owner
        ;
    """
    utl.query_postgres(sql)


######################### Insider, circles, insights, Posts #########################

# insert into insider (id)
# select owner as id
# from owner_collection_total_worth source
# left join insider target
#     on source.owner = target.id
# where source.owner_rank <= 200
#     and target.id is null
# group by 1;


def update_circle_insider():  # insider_to_circle_mapping
    sql = """

drop table if exists insider_staging;
create table insider_staging as
select
    owner as insider_id
    , owner_rank
    , 1 as circle -- 'top 200 whales'
from owner_collection_total_worth
where 1=1
    and owner in ( -- making sure the top 3 most valuable collections not excceeding 90% total worth
        select
            owner
        from owner_collection_total_worth
        where rnk <= 3
        group by 1
        having sum(collection_pct_total) < 0.9
    )
group by 1,2,3
order by owner_rank
limit 200
;

insert into insider (id)
select source.insider_id as id
from insider_staging source
left join insider target
    on source.insider_id = target.id
where target.id is null
;

update insider_to_circle_mapping
set is_current = false
where created_at < (select max(created_at) from insider_to_circle_mapping)
;

delete from insider_to_circle_mapping where is_current;

insert into insider_to_circle_mapping
select
    insider_id
    , owner_rank
    , circle
    , date(now() - interval '1 day') as created_at
    , true as is_current
from insider_staging
;

update insider_to_circle_mapping
set is_current = true
where created_at = (select max(created_at) from insider_to_circle_mapping)
;

drop table if exists insider_staging;

    """
    utl.query_postgres(sql=sql)


def update_insight():  # insight -- insider acquisitions
    sql = """
insert into insight_trx
with cet as (
    select
        c.insider_id
        , t.contract as collection_id
        , t.timestamp
        , t.token_id
        , t.action
        , t.trx_hash
        , eth_value_per_token
    from insider_to_circle_mapping c
    join nft_trx_union t
        on c.insider_id = t.to_address
    where action in ('mint', 'trade')
        and c.is_current -- current insiders only
        and t.timestamp > (select max(timestamp) from insight_trx)
        and t.eth_value_per_token > 0 -- making sure they actually spent their eth
    union all
    select
        insider_id
        , collection_id
        , timestamp
        , token_id
        , action
        , trx_hash
        , eth_value_per_token
    from insight_trx
)
select
    s.insider_id
    , s.collection_id
    , s.timestamp
    , s.token_id
    , s.action
    , s.trx_hash
    , s.eth_value_per_token
    , row_number() over (
        partition by s.insider_id, s.collection_id order by s.timestamp
    ) as nth_trx -- the nth acquisition of the same insider and collection
    , date(now() - interval '1 day') as created_at
from cet s
left join insight_trx t
    on s.trx_hash = t.trx_hash
    and s.token_id = t.token_id
where date(now() - interval '1 day') > (select max(created_at) from insight_trx)
    and s.timestamp >= (select max(timestamp) from insight_trx)
    and t.trx_hash is null
;
truncate table insight;
insert into insight
select
	insider_id
	, collection_id
	, min(timestamp) as started_at
	, sum(eth_value_per_token) as total_eth_spent
from insight_trx
group by 1,2
;
    """
    utl.query_postgres(sql=sql)


def update_circle_collection():  # contract_to_circle_mappin find new contracts that belongs to each circle
    sql = """
insert into collection_to_circle_mapping
with new as (
	select
		collection_id
		, circle_id
		, date(started_at)
	from (
		select
			c.circle_id
			, i.collection_id
			, i.started_at
			, row_number() over (partition by i.collection_id order by i.started_at) as nth_insider
		from insider_to_circle_mapping c
		join insight i
			on c.insider_id = i.insider_id
		where c.is_current
            and c.circle_id = 1 -- top 200 whales
	) a
		where nth_insider = 3 -- register when there are the three insider or more
)
select
	new.*
from new
left join collection_to_circle_mapping old
	on new.circle_id = old.circle_id
	and new.collection_id = old.collection_id
where old.collection_id is null
;
    """
    utl.query_postgres(sql=sql)


def update_post():
    sql = """
    insert into post (collection_id, created_at)
    select
        source.collection_id
        , date(source.created_at) + interval '1 day'
    from collection_to_circle_mapping source
    left join post target
        on source.collection_id = target.collection_id
    where source.created_at >= '2021-06-01'
        and target.collection_id is null
    ;
    """
    utl.query_postgres(sql=sql)
