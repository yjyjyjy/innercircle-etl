import update_etl as up
import etl_utls as utl
# date_gaps = utl.check_table_for_date_gaps(table="nft_trades", start_date="2021-12-01", end_date='2021-12-31')
# date_gaps = utl.check_table_for_date_gaps(table="nft_trades", start_date="2022-01-01", end_date="2022-01-31")
# date_gaps = utl.check_table_for_date_gaps(table="nft_trades", start_date="2022-02-01", end_date="2022-02-14")
# # date_gaps = utl.check_table_for_date_gaps(table="nft_trades", start_date="2022-02-16", end_date="2022-03-01")

# date_gaps = utl.check_table_for_date_gaps(
#     table="nft_trx_union", start_date="2022-02-01", end_date='2022-03-10')
# for date in date_gaps:
#     up.update_nft_trx_union(date)


date_gaps = utl.check_table_for_date_gaps(
    table="nft_trx_union"
    , start_date='2021-12-01'
    , end_date='2021-12-31'
    )
for date in date_gaps:
    up.update_nft_trx_union(date)