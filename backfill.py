import update_etl as up
import etl_utls as utl
# date_gaps = utl.check_table_for_date_gaps(table="nft_trades", start_date="2021-12-01", end_date='2021-12-31')
# date_gaps = utl.check_table_for_date_gaps(table="nft_trades", start_date="2022-01-01", end_date="2022-01-31")
# date_gaps = utl.check_table_for_date_gaps(table="nft_trades", start_date="2022-02-01", end_date="2022-02-14")
# # date_gaps = utl.check_table_for_date_gaps(table="nft_trades", start_date="2022-02-16", end_date="2022-03-01")
# date_gaps = utl.check_table_for_date_gaps(table="nft_trades", start_date="2021-01-01")
# for date in date_gaps:
#     up.update_nft_trade_opensea(date, use_upsert=False)

# date_gaps = utl.check_table_for_date_gaps(table="eth_token_transfers_2022", start_date="2022-01-01")
# for date in date_gaps:
#     up.update_token_transfers(date, running_in_cloud=utl.RUNNING_IN_CLOUD, use_upsert=False)


################  Three source tables  ################
# transactions
date_gaps = utl.check_table_for_date_gaps(table="eth_transactions", start_date="2022-01-01")
for date in date_gaps:
    up.update_eth_transactions(date)

# contracts
up.update_contracts()

# trades
# TODO don't hardcode this
date_gaps = utl.check_table_for_date_gaps(table="nft_trades", start_date="2022-01-01")
for date in date_gaps:
    up.update_nft_trade_opensea(date, use_upsert=False)

# token transfers
# TODO don't hardcode this
date_gaps = utl.check_table_for_date_gaps(table="eth_token_transfers_2022", start_date="2022-01-01")
for date in date_gaps:
    up.update_token_transfers(date, running_in_cloud=utl.RUNNING_IN_CLOUD, use_upsert=False)