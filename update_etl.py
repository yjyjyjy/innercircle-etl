from asyncio import subprocess
from address_metadata.address_metadata_worker import ADDRESS_META_TODO_FILE
import datetime
import decode_utls as dec
import etl_utls as utl
import glob
import json
import os
import pandas as pd
import subprocess
import time
OPENSEA_ABI_FILE_NAME = os.environ.get("OPENSEA_ABI_FILE_NAME")
from const import OPENSEA_TRADING_CONTRACT_V1, OPENSEA_TRADING_CONTRACT_V2


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

def update_address_metadata_trading_currency():

    utl.query_postgres(sql='''
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
    ''')


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
        table="eth_contracts",
        use_upsert=True,
        key='address'
    )


def update_contract_is_nft():
    sql = """
        update eth_contracts as con
        set is_nft = true
        FROM (
                select
                    tran.contract as address
                from nft_trades trade
                join eth_token_transfers_2022 tran -- hard coded
                    on trade.trx_hash = tran.trx_hash
                where trade.timestamp >= date(now() - interval '7 days')
                    and tran.timestamp >= date(now() - interval '7 days')
                    and tran.contract not in (
                        select
                            id
                        from address_metadata
                        where special_address_type = 'currency'
                        group by 1
                    )
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
        , c.id is null as missing_metadata
    from eth_contracts sot
    left join collection c
        on sot.address = c.id
    where sot.is_nft
        and c.id is null
        and sot.timestamp >= '2021-01-01'
        and sot.address not in (
                select
                    id
                from address_metadata
                where special_address_type = 'currency'
                group by 1
            )
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
        "slug",
        "last_updated_at"
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

def update_first_acquisition():
    max_ts = utl.get_terminal_ts(table='first_acquisition', end='max', key="timestamp")
    utl.query_postgres(sql=f'''
        drop table if exists cet_source;
        create table cet_source as
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
            where timestamp > '{max_ts}'
        ) a
        where rnk = 1;

        create index cet_source_idx on cet_source (contract, to_address);

        insert into first_acquisition
        select
            s.*
        from cet_source s
        left join first_acquisition t
            on s.contract = t.contract
            and s.to_address = t.to_address
        where t.contract is null
        ;

        drop table if exists cet_source;
    ''')

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

# The EOD ownership of all the tokens based on token transfer data
# Schema ["contract", "token_id", "owner"]
def update_nft_ownership(): # recreate script in the schema table
    print("🦄🦄 update_nft_ownership")
    sql = f"""

    drop table if exists cet_updated_trx;
    create table cet_updated_trx as
    select
        contract
        , token_id
        , to_address
        , timestamp
    from (
        select
            contract
            , token_id
            , to_address
            , timestamp
            , row_number() over (partition by contract, token_id order by timestamp desc) as rnk
        from nft_trx_union
        where timestamp > (select max(last_transferred_at) from nft_ownership)
    ) a
    where rnk = 1
        and contract in (
            select id
            from collection
            group by 1
        )
    ;

    create index cet_updated_trx_idx_contract_token_id on cet_updated_trx (contract, token_id);

    update nft_ownership t
    set address = s.to_address
        , last_transferred_at = s.timestamp
    from cet_updated_trx s
    where s.contract = t.contract
        and s.token_id = t.token_id
    ;

    insert into nft_ownership
    select
        s.contract
        , s.token_id
        , s.to_address as address
        , s.timestamp as last_transferred_at
    from cet_updated_trx s
    left join nft_ownership t
        on s.contract = t.contract
        and s.token_id = t.token_id
    where t.contract is null
    ;

    drop table if exists cet_updated_trx;
    ;"""
    utl.query_postgres(sql)

# each owner's worth by each contract they own. Plus their total worth
def update_address_collection_total_worth():

    sql = f"""
        drop table if exists cet_address_collection_worth_ranked;
        create table cet_address_collection_worth_ranked as
        with base as (
            select
                o.address
                , o.contract
                , p.floor_price_in_eth
                , count(distinct o.token_id) as num_tokens
            from nft_ownership o
            join nft_contract_floor_price p
                on p.contract = o.contract
            where p.date = (select max(date) from nft_contract_floor_price)
                and o.address not in (
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
        , address_collection_worth as (
            select
                address
                , contract
                , num_tokens
                , floor_price_in_eth
                , floor_price_in_eth * num_tokens as collection_worth
            from base
        )
        select
            address
            , contract
            , num_tokens
            , floor_price_in_eth
            , collection_worth
            , row_number() over (partition by address order by collection_worth desc) as rnk
        from address_collection_worth
        ;

        create index cet_address_collection_worth_ranked_idx_address on cet_address_collection_worth_ranked (address);

        drop table if exists cet_address_worth_ranked;
        create table cet_address_worth_ranked as
        with address_worth as (
            select
                address
                , sum(collection_worth) as total_worth
            from cet_address_collection_worth_ranked
            group by 1
        )
        select
            address
            , total_worth
            , RANK () OVER (ORDER BY total_worth desc) address_rank
        from address_worth
        ;

        create index cet_address_worth_ranked_idx_address on cet_address_worth_ranked (address);

        drop table if exists address_collection_total_worth;
        create table address_collection_total_worth as
        select
            ocwr.address
            , address_rank
            , ocwr.contract
            , ocwr.num_tokens
            , ocwr.floor_price_in_eth
            , ocwr.collection_worth
            , rnk
            , total_worth
            , case when total_worth=0 then null else coalesce(ocwr.collection_worth,0) / total_worth end as collection_pct_total
        from cet_address_collection_worth_ranked ocwr
        join cet_address_worth_ranked owr
            on ocwr.address = owr.address
        left join address_metadata m
            on owr.address = m.id
            and m.is_special_address
        left join eth_contracts c
            on owr.address = c.address
        where m.id is null -- no special addresses
            and c.address is null  -- no contracts
        ;

        create index address_collection_total_worth_idx_address on address_collection_total_worth (address);

        drop table if exists cet_address_collection_worth_ranked;
        drop table if exists cet_address_worth_ranked;
    """
    utl.query_postgres(sql)

# most profitable traders in the past 90 days

def update_past_90_days_trading_roi():
    utl.query_postgres(sql = '''
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
            and price_per_token > 0 -- making sure the sale is not some tricky transaction (wash sale etc)
            and trade_payment_token in ('ETH', 'WETH')
        ;
    ''')

    utl.query_postgres(sql = '''
        drop table if exists cet_buy;

        create table cet_buy as
        select
            to_address as address
            , date
            , contract
            , token_id
            , floor_price_in_eth
            , price_per_token
        from trx_with_floor_price
        group by 1,2,3,4,5,6
        ;

        create index buy_idx_address_contract_token_id on cet_buy (address, contract, token_id);
    ''')

    utl.query_postgres(sql = '''
        drop table if exists cet_sell;

        create table cet_sell as
        select
            from_address as address
            , date
            , contract
            , token_id
            , floor_price_in_eth
            , price_per_token
        from trx_with_floor_price
        where floor_price_in_eth > 0  -- meaningful project
        group by 1,2,3,4,5,6
        ;

        create index sell_idx_address_contract_token_id on cet_sell (address, contract, token_id);
    ''')

    utl.query_postgres(sql = '''
        drop table if exists trade_roi_flat;

        create table trade_roi_flat as
        with cet as (
            select
                buy.address
                , buy.date as buy_date
                , buy.contract
                , buy.token_id
                , buy.floor_price_in_eth as buy_floor_price
                , buy.price_per_token as buy_eth_amount
                , sell.floor_price_in_eth as sell_floor_price
                , sell.price_per_token as sell_eth_amount
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
                    ) f
                where rnk = 1
            ) fp -- current floor price
                on fp.contract = buy.contract
        )
        select
            address
            , buy_date
            , contract
            , token_id
            , buy_eth_amount
            , coalesce(buy_floor_price, buy_eth_amount) as acquisition_floor_price
            , sell_floor_price
            , current_floor_price
            , coalesce(sell_floor_price, current_floor_price, 0) - coalesce(buy_floor_price, buy_eth_amount) as gain -- 🔥 def of gain
        from cet
        where rnk = 1
        ;

        create index trade_roi_flat_idx_address on trade_roi_flat (address);
    ''')

    utl.query_postgres(sql = '''
        -- staging table before appending wallet total gain
        drop table if exists cet_roi;
        create table cet_roi as
        select
            address
            , contract
            , buy_date
            , buy_eth_amount
            , gain
            , gain*1.0/buy_eth_amount as roi_pct
            , row_number() over (partition by address order by gain desc) as collection_gain_rank_in_portfolio
        from (
            select
                roi.address
                , roi.contract
                , min(buy_date) as buy_date
                , sum(buy_eth_amount) as buy_eth_amount
                , sum(gain) as gain
            from trade_roi_flat roi
            left join address_metadata m
                on roi.address = m.id
                and m.is_special_address
            left join eth_contracts c
                on roi.address = c.address
            where m.id is null
                and c.address is null
            group by 1,2
        ) a
        ;

        create index cet_roi_idx_address on cet_roi (address);
    ''')

    utl.query_postgres(sql = '''
        truncate table past_90_days_trading_roi;

        insert into past_90_days_trading_roi
        with total_gain as (
            select
                address
                , sum(gain) as total_gain
            from cet_roi
            group by 1
        )
        select
            roi.address
            , roi.contract
            , roi.buy_date
            , roi.buy_eth_amount
            , roi.gain
            , roi.roi_pct
            , roi.collection_gain_rank_in_portfolio
            , t.total_gain
        from cet_roi roi
        join total_gain t
            on roi.address = t.address
        ;

        drop table if exists cet_roi;

        -- drop table if exists trx_with_floor_price;
        -- drop table if exists cet_buy;
        -- drop table if exists cet_sell;
        -- drop table if exists trade_roi_flat;
    ''')

    utl.query_postgres(sql = '''
    truncate table insider_past_90_days_trading_roi;
    insert into insider_past_90_days_trading_roi
    select
        *
    from past_90_days_trading_roi
    where address in (
            select id
            from insider
            group by 1
        )
        and contract in (
            select id
            from collection
            group by 1
        )
    ;
    ''')

######################### Insider, circles, insights, Posts #########################

def update_circle_insider():  # insider_to_circle_mapping

    sql = """
        drop table if exists insider_staging;
        create table insider_staging as
        select
            address as insider_id
            , address_rank
            , 1 as circle -- 'top 200 whales'
        from address_collection_total_worth
        where 1=1
            and address in ( -- making sure the top 3 most valuable collections not excceeding 90% total worth
                select
                    address
                from address_collection_total_worth
                where rnk <= 3
                group by 1
                having sum(collection_pct_total) < 0.9
            )
        group by 1,2,3
        order by address_rank
        limit 200
        ;

        insert into insider_staging
        with addresses as (
            select
                address
            from (
                select
                    address
                    , avg(total_gain) as total_gain
                    , sum(gain)/avg(total_gain) as pct
                from past_90_days_trading_roi
                where collection_gain_rank_in_portfolio = 1
                    and total_gain > 0
                    and gain > 0
                group by 1
            ) a
            where pct < .9 -- avoid one time wonders
            order by total_gain desc
            limit 200
        )
        select
            a.address
            , w.address_rank
            , 2 as circle -- 'most profitable traders last 90 days'
        from addresses a
        left join (
            select
                address as address
                , min(address_rank) as address_rank
            from address_collection_total_worth
            group by 1
        ) w
            on a.address = w.address
        ;


        insert into insider (id)
        select source.insider_id as id
        from insider_staging source
        left join insider target
            on source.insider_id = target.id
        where target.id is null
        group by 1
        ;

        delete from insider_to_circle_mapping where created_at = date(now() - interval '1 day');

        update insider_to_circle_mapping
        set is_current = false
        ;

        insert into insider_to_circle_mapping
        select
            insider_id
            , address_rank
            , circle
            , date(now() - interval '1 day') as created_at
            , true as is_current
        from insider_staging
        ;

        drop table if exists insider_staging;

    """
    utl.query_postgres(sql=sql)

def update_insider_portfolio():
    utl.query_postgres(sql='''
    truncate table insider_portfolio;
    insert into insider_portfolio
    select
        i.id as insider_id
        , o.address_rank
        , o.contract as collection_id
        , o.num_tokens
        , o.floor_price_in_eth
        , o.collection_worth
        , o.rnk as collection_rank_in_portfolio
        , o.total_worth
        , o.collection_pct_total
    from insider i
    join address_collection_total_worth o -- what they own
        on i.id = o.address
    ;
    ''')

def update_insight_trx():
    utl.query_postgres(sql='''
    truncate table insight_trx;
    insert into insight_trx
	select
		date
		, i.insider_id
		, cast('buy' as varchar) as action
		, contract as collection_id
		, floor_price_in_eth
		, count(distinct t.token_id) as num_tokens
		, sum(price_per_token) as total_eth_amount
	from insider_to_circle_mapping i
	join cet_buy t
		on i.insider_id = t.address
		and i.is_current
		and t.contract in (
        select id from collection group by 1
        )
	group by 1,2,3,4,5
	union all
	select
		date
		, i.insider_id
		, cast('sell' as varchar) as action
		, contract as collection_id
		, floor_price_in_eth
		, count(distinct t.token_id) as num_tokens
		, sum(price_per_token) as total_eth_amount
	from insider_to_circle_mapping i
	join cet_sell t
		on i.insider_id = t.address
		and i.is_current
		and t.contract in (
        select id from collection group by 1
        )
	group by 1,2,3,4,5
    ;
    ''')

def update_insight():  # insifght -- insider acquisitions
    utl.query_postgres(sql="""
        truncate table insight;
        insert into insight
        with trx as (
            select
                insider_id
                , collection_id
                , action
                , sum(num_tokens) as num_tokens
                , sum(total_eth_amount) as total_eth_amount
                , max(date) as last_traded_at
            from insight_trx
            where insider_id in (
                    select insider_id
                    from insider_to_circle_mapping
                        where circle_id = 2
                            and is_current
                    group by 1
                )
                and date >= date(now() - interval '7 day')
            group by 1,2,3
        )
        , gain as (
            select
                address
                , max(total_gain) as past_90_days_trading_gain -- should only have 1 per address but just to be safe
            from past_90_days_trading_roi
            group by 1
        )
        , accuracy as (
            select
                address
                , count(distinct case when gain > 0 then contract end)*1.0
                    /count(distinct contract)
                as pct_trades_profitable
            from past_90_days_trading_roi
            group by 1
        )
        , endorsement as (
            select
                i.id as insider_id
                , f.contract
                , min(timestamp) as timestamp
            from first_acquisition f
            join insider i
                on f.to_address = i.id
            group by 1,2
        )
        , circle_first as (
            select
                m.circle_id
                , e.contract
                , min(e.timestamp) as timestamp
            from endorsement e
            join insider_to_circle_mapping m
                on e.insider_id = m.insider_id
            where circle_id = 2 -- 🤯 temp hack. Only do circle 2 for now.
            group by 1,2
        )
        , base as (
            select
                -- display factors
                trx.insider_id
                , trx.collection_id
                , trx.action
                , trx.num_tokens
                , trx.total_eth_amount
                , trx.last_traded_at
                , coalesce(p.num_tokens, 0) as num_tokens_owned
                -- ranking inputs
                , power(.8, date(now())-cast(trx.last_traded_at as date) + 1) as insight_time_decay
                , coalesce(g.past_90_days_trading_gain, 0) as past_90_days_trading_gain
                , coalesce(accu.pct_trades_profitable, 0) as pct_trades_profitable
                , cir_fir.timestamp as circle_collection_first_ts
                , per_fir.timestamp as insider_collection_first_ts
                , power(.8, date(now())-cast(cir_fir.timestamp as date) + 1) as circle_collection_first_time_decay
                , power(.8, date(now())-cast(per_fir.timestamp as date) + 1) as insider_collection_first_time_decay
            from trx
            left join insider_portfolio p
                on trx.insider_id = p.insider_id
                and trx.collection_id = p.collection_id
            left join gain g
                on trx.insider_id = g.address
            left join accuracy accu
                on trx.insider_id = accu.address
            left join circle_first cir_fir -- it should be a inner join but use left join to detect any issues.
                on cir_fir.contract = trx.collection_id
            left join endorsement per_fir
                on per_fir.contract = trx.collection_id
                and per_fir.insider_id = trx.insider_id
        )
        select
            *
            , past_90_days_trading_gain/(select max(total_eth_amount) from trx) * 2 -- gain factor x2 is the weight booster
                + pct_trades_profitable * 1.5 -- accuracy factor
                + insight_time_decay * 1.2 -- recency / freshness
                + circle_collection_first_time_decay -- being first
                + insider_collection_first_time_decay -- new endorser
                as feed_importance_score
        from base
        ;
    """)

############# SUSPENDED ##################
def update_circle_collection():
# contract_to_circle_mappin find new contracts that belongs to each circle
    sql = """
        insert into collection_to_circle_mapping
        with new as (
            select
                collection_id
                , circle_id
                , date(started_at) as started_at
            from (
                select
                    i.collection_id
                    , c.circle_id
                    , min(date) as started_at
                from insider_to_circle_mapping c
                join insight_trx i
                    on c.insider_id = i.insider_id
                    and i.action = 'buy'
                where c.is_current
                group by 1,2
            ) a
        )
        select
            new.collection_id
            , new.circle_id
            , new.started_at
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
    insert into post (collection_id, created_at, feed_importance_score)
    select
        source.collection_id
        , min(date(source.started_at)) + interval '1 day'
        , 0 -- default value
    from collection_to_circle_mapping source
    left join post target
        on source.collection_id = target.collection_id
    where source.started_at >= '2021-06-01'
        and target.collection_id is null
        and source.circle_id = 2 -- 🤯 hack: 1 is diabled for now
    group by 1
    ;

    update post
    set feed_importance_score = 0;

    update post
    set feed_importance_score = fis.feed_importance_score
    from (
        select
            collection_id
            , sum(feed_importance_score) as feed_importance_score
        from (
            select
                collection_id
                , feed_importance_score
                , row_number() over (partition by collection_id order by feed_importance_score desc) as rnk
            from insight -- 🤯 hack: 1 is diabled for now
        ) a
        where rnk <= 3 -- only the most important 3 insights bc insights beyond that are likely hidden by UI
        group by 1
    ) fis
    where post.collection_id = fis.collection_id
    ;
    """
    utl.query_postgres(sql=sql)

def update_address_metadata_is_contract():

    utl.query_postgres(sql='''
        update address_metadata u
        set is_contract = true
        from eth_contracts c
        where c.address = u.id
        ;
        update address_metadata
        set is_contract = false
        where is_contract is null
        ;
    ''')

def update_address_metadata_trader_profile():

    utl.query_postgres(sql='''
        insert into address_metadata (id)
        select s.id
        from insider s
        left join address_metadata t
            on t.id = s.id
        where t.id is null
        ;
    ''')

    update_address_metadata_is_contract()

    df = utl.query_postgres(sql='''
        select
            m.id
        from address_metadata m
        join insight i
            on m.id = i.insider_id
        where 1=1 -- i.feed_importance_score > 0
            and (
                m.last_updated_at is null
                or m.last_updated_at < now() - interval '7 days'
                )
        group by 1
        ;
        ''', columns=['id'])

    df.to_csv(f'address_metadata/{ADDRESS_META_TODO_FILE}', index=False, header=False)

    print(f'🦄🦄 scraping address metadata: {datetime.datetime.now()}')
    subprocess.call('./cron_exec_address_metadata.sh')
    print(f'💪💪 completed scraping address metadata: {datetime.datetime.now()}')

    # mw.ADDRESS_META_FINISHED_FILE
    # for now just grab all files and do upsert
    files = glob.glob('./address_metadata/metadata/*')

    output = pd.DataFrame()
    for file in files:

        with open(file) as json_file:
            data = json.load(json_file)

        try:
            meta = parse_metadata_json(data)
            row = pd.DataFrame(meta, index=[0])
            if output.empty:
                output = row
            else:
                output = output.append(row)
        except:
            print(f"🤯🤯 error parsing address metadata json file: {file}")

    utl.query_postgres(sql='truncate table address_metadata_opensea;')
    utl.copy_from_df_to_postgres(df = output, table='address_metadata_opensea', csv_filename_with_path=None, use_upsert=True, key='id')
    utl.query_postgres(sql='''
        update address_metadata t
        set
            id = s.id
            , opensea_display_name = s.opensea_display_name
            , opensea_image_url = s.opensea_image_url
            , opensea_banner_image_url = s.opensea_banner_image_url
            , opensea_bio = s.opensea_bio
            , twitter_username = s.twitter_username
            , instagram_username = s.instagram_username
            , website = s.website
            , opensea_user_created_at= s.opensea_user_created_at
            , last_updated_at= s.last_updated_at
        from address_metadata_opensea s
        where s.id = t.id
        ;
    ''')

    # move data into insider_id which is a production table serving Next.js
    utl.query_postgres(sql='''
    truncate table insider_metadata;
    insert into insider_metadata
    select
        id as insider_id
        , public_name_tag
        , opensea_display_name
        , opensea_image_url
        , opensea_banner_image_url
        , opensea_bio
        , ens
        , twitter_username
        , instagram_username
        , medium_username
        , email
        , website
        , opensea_user_created_at
        , last_updated_at
    from address_metadata
    where id in (
        select id
        from insider
    );
    ''')

def parse_metadata_json(data):
    meta = {}
    meta['id'] = data['address']
    meta['opensea_display_name'] = data['displayName'] or data['user']['publicUsername']
    # datetime.datetime.strptime(data['createdDate'].split('.')[0], '%Y-%m-%dT%H:%M:%S') #"createdDate": "2021-03-13T05:48:10.653999",
    meta['opensea_image_url'] = data['imageUrl']
    meta['opensea_banner_image_url'] = data['bannerImageUrl']
    meta['opensea_bio'] = data['bio']
    meta['twitter_username'] = data['metadata']['twitterUsername']
    meta['instagram_username'] = data['metadata']['instagramUsername']
    meta['website'] = data['metadata']['websiteUrl']
    meta['opensea_user_created_at'] = data['createdDate']
    meta['last_updated_at'] = datetime.datetime.now()
    return meta



# def load_meta_data_from_file():
