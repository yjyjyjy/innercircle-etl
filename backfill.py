import update_etl as up
import etl_utls as utl

# backfilling nft_trx_union Dec. 2021
# backfilling nft_trades Jan 2021 - Oc. 2021

# delete nft_trx_union Jan. 2021 - Nov. 2021
utl.query_postgres(sql="delete from nft_trx_union where timestamp >= '2020-12-01' and timestamp < '2021-12-01';")
# backfilling nft_trx_union Jan. 2021 - Nov. 2021
date_gaps = utl.check_table_for_date_gaps(
    table="nft_trx_union"
    , start_date='2020-12-01'
    )
for date in date_gaps:
    up.update_nft_trx_union(date)
# delete nft_contract_floor_price Jan. 2021 - Nov. 2021
utl.query_postgres(sql="delete from nft_contract_floor_price where date >= '2021-01-01' and date < '2021-12-02';")
# backfilling nft_contract_floor_price Jan. 2021 - Nov. 2021
date_gaps = utl.check_table_for_date_gaps(
    table="nft_contract_floor_price"
    , start_date='2021-01-01'
    , key="date"
    )
for date in date_gaps:
    up.update_nft_contract_floor_price(date)

up.update_nft_ownership()
up.update_owner_collection_total_worth()