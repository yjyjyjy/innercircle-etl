import update_etl as up
import etl_utls as utl

up.update_past_90_days_trading_roi()

######## Insider, circles, insights ##########
up.update_circle_insider() # overwrite the previous day
up.update_insider_portfolio() # out of all addresses ever tagged as as insiders, what do they currently
up.update_insight_trx()
up.update_insight()
up.update_circle_collection()
up.update_post()
