import update_etl as up
import etl_utls as utl

################  Three source tables  ################
# transactions

# up.load_address_metadata_from_json()
up.update_address_metadata_trader_profile(complete_update = True)