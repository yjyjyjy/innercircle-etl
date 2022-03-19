import update_etl as up
import etl_utls as utl

################  Three source tables  ################
# transactions
date_gaps = utl.check_table_for_date_gaps(
    table="eth_transactions"
    , start_date=utl.get_previous_day(num_days=7)
    )
for date in date_gaps:
    up.update_eth_transactions(date)

# contracts
date_gaps = utl.check_table_for_date_gaps(
    table="eth_contracts"
    , start_date=utl.get_previous_day(num_days=7)
    )
for date in date_gaps:
    up.update_contracts(date=date)

# trades
# TODO don't hardcode this
date_gaps = utl.check_table_for_date_gaps(
    table="nft_trades"
    , start_date=utl.get_previous_day(num_days=7)
    )
for date in date_gaps:
    up.update_nft_trade_opensea(date, use_upsert=False)

up.update_address_metadata() # add currency to address metadata

# token transfers
# TODO don't hardcode this
date_gaps = utl.check_table_for_date_gaps(
    table="eth_token_transfers_2022"
    , start_date=utl.get_previous_day(num_days=7)
    )
for date in date_gaps:
    up.update_token_transfers(date, running_in_cloud=utl.RUNNING_IN_CLOUD, use_upsert=False)

################ is_NFT filed and NFT contracts ############
# update is_NFT
up.update_contract_is_nft()
up.mark_new_contracts()
up.update_collection()
# up.update_nft_contract_abi()

################ trx_union ############
date_gaps = utl.check_table_for_date_gaps(
    table="nft_trx_union"
    , start_date=utl.get_previous_day(num_days=7)
    )
for date in date_gaps:
    up.update_nft_trx_union(date)

up.update_first_acquisition()

################ floor price ############
date_gaps = utl.check_table_for_date_gaps(
    table="nft_contract_floor_price"
    , start_date=utl.get_previous_day(num_days=7)
    , key="date")
for date in date_gaps:
    up.update_nft_contract_floor_price(date)


################ ownership and floor price ############
up.update_nft_ownership() # incremental updating ownership
up.update_address_collection_total_worth()
up.update_past_90_days_trading_roi()

######## Insider, circles, insights ##########
up.update_circle_insider() # overwrite the previous day
up.update_insider_portfolio() # out of all addresses ever tagged as as insiders, what do they currently
up.update_insight_trx()
up.update_insight()
up.update_circle_collection()
up.update_post()
