import update_etl as up
import etl_utls as utl



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

################ is_NFT filed and NFT contracts ############
# update is_NFT
up.update_contract_is_nft()
up.mark_new_contracts()
up.update_collection()
# up.update_nft_contract_abi()

################ trx_union ############
up.update_nft_trx_union()

################ down funnel tables ############
up.update_nft_ownership()
# TODO don't hardcode this
date_gaps = utl.check_table_for_date_gaps(table="nft_contract_floor_price", start_date="2022-01-01", key="date")
for date in date_gaps:
    up.update_nft_contract_floor_price(date)

up.update_owner_collection_total_worth()
up.update_past_90_days_trading_roi()

######## Insider, circles, insights ##########
up.update_circle_insider()
up.update_insight()
up.update_circle_collection()
up.update_post()
