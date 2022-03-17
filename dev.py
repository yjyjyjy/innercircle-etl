import update_etl as up
import etl_utls as utl
date_gaps = utl.check_table_for_date_gaps(
    table="nft_trx_union"
    , start_date='2021-12-21'
    , end_date='2021-12-31'
    )
for date in date_gaps:
    up.update_nft_trx_union(date)

# date_gaps = utl.check_table_for_date_gaps(
#     table="nft_trades"
#     , start_date='2021-11-01'
#     )

# backfilling nft_trx_union Dec. 2021
# backfilling nft_trades Jan 2021 - Oc. 2021

# TODO
# backfilling nft_contract_floor_price Dec. 2021 - now

# TODO
# delete nft_trx_union Jan. 2021 - Nov. 2021
# backfilling nft_trx_union Jan. 2021 - Nov. 2021
# delete nft_contract_floor_price Jan. 2021 - Nov. 2021
# backfilling nft_contract_floor_price Jan. 2021 - Nov. 2021