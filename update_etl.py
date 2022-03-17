import pandas as pd
import etl_utls as utl
import decode_utls as dec
from const import OPENSEA_TRADING_CONTRACT_V1, OPENSEA_TRADING_CONTRACT_V2
import time
import os
OPENSEA_ABI_FILE_NAME = os.environ.get("OPENSEA_ABI_FILE_NAME")


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
    SELECT  -- trx_hash is not the unique key for token transfers table!!!
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
    price = dec.get_opensea_trade_price(date)
    currency = dec.get_opensea_trade_currency(date)
    df = pd.merge(currency, price, on='trx_hash')
    df = df[['timestamp', 'trx_hash', 'eth_value', 'payment_token', 'price', 'platform']]

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


def update_contracts(date):
    print("🦄🦄 start update_contracts")
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
        where date(block_timestamp) = '{date}'
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
        table="eth_contracts"
        # not using upsert because of primary key constraint
    )


def update_contract_is_nft():
    sql = """
        update eth_contracts as con
        set is_nft = true
        FROM (
                select
                    tran.contract as address
                from nft_trades trade
                join eth_token_transfers_2022 tran
                    on trade.trx_hash = tran.trx_hash
                where trade.timestamp >= '2022-01-01'
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
    select
        sot.address
        , meta.id is null as missing_metadata
    from eth_contracts sot
    left join collection meta
        on sot.address = meta.id
    where sot.is_nft
        and meta.id is null
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

            if status_code in [429, 404, 495]:
                print(f"⏱ current wait_time: {wait_time}")
                time.sleep(60)
                if wait_time <= 5:
                    wait_time += 0.5

            if status_code == 200:
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

def update_nft_trx_union(date):
    utl.delete_current_day_data(date=date, table='nft_trx_union', key='timestamp')
    year = str(date)[:4]

    utl.query_postgres(sql = f'''
        drop table if exists cet_nft_token_transfers;
        create table cet_nft_token_transfers as
        select
            trans.*
            , case when trade.payment_token = '0x0000000000000000000000000000000000000000' then 'ETH'
                when trade.payment_token = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' then 'WETH'
                when trade.payment_token = '0x64d91f12ece7362f91a6f8e7940cd55f05060b92' then 'ASH'
                when trade.payment_token = '0x15d4c048f83bd7e37d49ea4c83a07267ec4203da' then 'GALA'
                when trade.payment_token = '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48' then 'USDC'
                when trade.payment_token = '0x5881da4527bcdc44a100f8ba2efc4039243d2c07' then 'LGBTQ'
                when trade.payment_token = '0x3845badade8e6dff049820680d1f14bd3903a5d0' then 'SAND'
                when trade.payment_token = '0x0f5d2fb29fb7d3cfee444a200298f468908cc942' then 'MANA'
            else trade.payment_token end as trade_payment_token
            , trade.price as trade_price
            , trade.platform as trade_platform
            , trx.eth_value
            , trx.from_address=trans.to_address as caller_is_receiver
        from eth_token_transfers_{year} trans
        join eth_contracts con
            on con.address = trans.contract
        join eth_transactions trx
            on trx.trx_hash = trans.trx_hash
            and trx.timestamp >= '{date}'
            and trx.timestamp < date('{date}') + interval '1 day'
        left join nft_trades trade
            on trade.trx_hash = trans.trx_hash
            and trade.timestamp >= '{date}'
            and trade.timestamp < date('{date}') + interval '1 day'
        where trans.timestamp >= '{date}'
            and trans.timestamp < date('{date}') + interval '1 day'
            and con.is_nft
        ;

        create index cet_nft_token_transfers_idx_trx_hash on cet_nft_token_transfers (trx_hash);

        drop table if exists cet_num_tokens;

        create table cet_num_tokens as
        select
            trx_hash
        , count(distinct token_id_or_value) as num_tokens_in_the_same_transaction
        from cet_nft_token_transfers
        group by 1
        ;

        create index cet_num_tokens_idx_trx_hash on cet_num_tokens (trx_hash);
    ''')

    df = utl.query_postgres(
        sql='''
        select
            trans.timestamp
            , trans.trx_hash
            , trans.contract
            , trans.token_id_or_value as token_id
            , trans.from_address
            , trans.to_address
            , trade_platform
            , trade_payment_token
            , mul.num_tokens_in_the_same_transaction
            , coalesce(trade_price, eth_value)/mul.num_tokens_in_the_same_transaction as price_per_token -- there shouldn't be div by zero
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
                when trade_platform is not null then 'trade'
                else 'transfer'
                end as action
            , caller_is_receiver
        from cet_nft_token_transfers trans
        join cet_num_tokens mul
            on trans.trx_hash = mul.trx_hash
        ;
        '''
        , columns=[
            "timestamp",
            "trx_hash",
            "contract",
            "token_id",
            "from_address",
            "to_address",
            "trade_platform",
            "trade_payment_token",
            "num_tokens_in_the_same_transaction",
            "eth_value_per_token",
            "action",
            "caller_is_receiver"
        ],
    )
    utl.query_postgres(sql='''
    drop table cet_nft_token_transfers;
    drop table cet_num_tokens;
    ''')
    utl.copy_from_df_to_postgres(df=df, table="nft_trx_union", use_upsert=False)



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
    create index nft_ownership_idx_contract_token_id on nft_ownership (contract, token_id);
    ;"""
    utl.query_postgres(sql)


def update_nft_contract_floor_price(date):
    print("🦄🦄 update_nft_contract_floor_price: " + date)
    sql = f"""
        insert into nft_contract_floor_price
        select
            cast('{date}' as date)
            , contract
            , percentile_disc(0.2) within group (order by price_per_token)
        from nft_trx_union
        where timestamp >= date('{date}') - interval'1 days'
            and timestamp < date('{date}') + interval'1 days'
            and action = 'trade'
            and trade_payment_token in ('ETH', 'WETH')
        group by 1,2
        ;
        --  having count(distinct trx_hash) >= 2  -- having more than 2 trades
    """
    utl.query_postgres(sql)

# each owner's worth by each contract they own. Plus their total worth
def update_owner_collection_total_worth():
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

# most profitable traders in the past 90 days

def update_past_90_days_trading_roi():
    sql = '''
    drop table if exists trx_with_floor_price;
    create table trx_with_floor_price as
    select
        to_address
        , from_address
        , date(t.timestamp) as date
        , t.contract
        , token_id
        , price_per_token
        , p.floor_price_in_eth
    from nft_trx_union t
    left join nft_contract_floor_price p
        on date(t.timestamp) = p.date
        and t.contract = p.contract
    where t.timestamp >= now() - interval'90 days'
    ;

    drop table if exists cet_buy;
    create table cet_buy as
    select
        to_address as address
        , date
        , contract
        , token_id
        , coalesce(floor_price_in_eth, price_per_token) as acquisition_floor_price
    from trx_with_floor_price
    where price_per_token > 0 -- not airdrop etc.
    group by 1,2,3,4,5
    ;
    create index buy_idx_address_contract_token_id on cet_buy (address, contract, token_id);

    drop table if exists cet_sell;
    create table cet_sell as
    select
        from_address as address
        , date
        , contract
        , token_id
        , floor_price_in_eth
    from trx_with_floor_price
    where floor_price_in_eth > 0  -- meaningful project
        and price_per_token > 0 -- making sure the sale is not some tricky transaction (wash sale etc)
    group by 1,2,3,4,5
    ;
    create index sell_idx_address_contract_token_id on cet_sell (address, contract, token_id);

    drop table if exists trade_roi_flat;
    create table trade_roi_flat as
    with cet as (
        select
            buy.address
            , buy.date as buy_date
            , buy.contract
            , buy.token_id
            , buy.acquisition_floor_price
            , sell.floor_price_in_eth as sell_floor_price
            , fp.floor_price_in_eth as current_floor_price
            , row_number() over (partition by buy.address, buy.contract, buy.token_id order by sell.date) as rnk
        from cet_buy buy
        left join cet_sell sell
            on buy.address = sell.address
            and buy.contract = sell.contract
            and buy.token_id = sell.token_id
            and sell.date >= buy.date
        left join (
            select
                contract
                , floor_price_in_eth
            from (
                select
                    contract
                    , floor_price_in_eth
                    , row_number() over (partition by contract order by date desc) as rnk
                from nft_contract_floor_price
                where date >= now() - interval'3 days'
            ) cet
            where rnk = 1
        ) fp -- current floor price
            on fp.contract = buy.contract
    )
    select
        address
        , buy_date
        , contract
        , token_id
        , acquisition_floor_price
        , sell_floor_price
        , current_floor_price
        , coalesce(sell_floor_price, current_floor_price) - acquisition_floor_price as gain
    from cet
    where rnk = 1
    ;
    create index trade_roi_flat_idx_address on trade_roi_flat (address);

    drop table if exists past_90_days_trading_roi;
    create table past_90_days_trading_roi as
    select
        address
        , sum(gain) as gain
    from trade_roi_flat
    group by 1
    ;

    -- drop table if exists trx_with_floor_price;
    -- drop table if exists cet_buy;
    -- drop table if exists cet_sell;
    -- drop table if exists trade_roi_flat;

    '''
    utl.query_postgres(sql)


######################### Insider, circles, insights, Posts #########################

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
        , price_per_token
    from insider_to_circle_mapping c
    join nft_trx_union t
        on c.insider_id = t.to_address
    where action in ('mint', 'trade')
        and c.is_current -- current insiders only
        and t.timestamp > (select max(timestamp) from insight_trx)
        and t.price_per_token > 0 -- making sure they actually spent their eth
        and (t.trade_payment_token is null or t.trade_payment_token in ('ETH', 'WETH'))
    union all
    select
        insider_id
        , collection_id
        , timestamp
        , token_id
        , action
        , trx_hash
        , price_per_token
    from insight_trx
)
select
    s.insider_id
    , s.collection_id
    , s.timestamp
    , s.token_id
    , s.action
    , s.trx_hash
    , s.price_per_token
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
	, sum(price_per_token) as total_eth_spent
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
