import update_etl as up
import etl_utls as utl


def check_table_for_date_gaps(table, start_date, end_date=None, key="timestamp"):
    dates = utl.get_date_list(start_date=start_date, end_date=end_date)
    end_date_clause = f"and {key} <= '{end_date}'" if end_date != None else ""
    sql = f"""
        select cast(date({key}) as varchar) as date
        from {table}
        where {key} >= '{start_date}'
            {end_date_clause}
        group by 1
    """
    uploaded = utl.query_postgres(sql, columns=["date"])
    uploaded = uploaded.date.to_list()

    gaps = [date for date in dates if date not in uploaded]
    gaps.sort()
    print(f"🦄🦄: {table} gaps:")
    print(gaps)
    return gaps


################  Three source tables  ################
# transactions
date_gaps = check_table_for_date_gaps(table="eth_transactions", start_date="2022-01-01")
for date in date_gaps:
    up.update_eth_transactions(date)

# contracts
up.update_contracts()

# trades
# TODO don't hardcode this
date_gaps = check_table_for_date_gaps(table="nft_trades", start_date="2022-01-01")
for date in date_gaps:
    up.update_nft_trade_opensea(date)

# token transfers
# TODO don't hardcode this
date_gaps = check_table_for_date_gaps(table="eth_token_transfers_2022", start_date="2022-01-01")
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
date_gaps = check_table_for_date_gaps(table="nft_contract_floor_price", start_date="2022-01-01", key="date")
for date in date_gaps:
    up.update_nft_contract_floor_price(date)

up.update_owner_collection_total_worth()

######################### Insider, circles, insights #########################
up.update_circle_insider()
up.update_insight()
up.update_circle_collection()
up.update_post()
