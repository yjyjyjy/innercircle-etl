import update_etl as up
import etl_utls as utl

up.update_address_metadata_trader_profile()


# import requests
# from requests.exceptions import ReadTimeout, SSLError, ProxyError
# from bs4 import BeautifulSoup
# from logging.handlers import TimedRotatingFileHandler
# from time import sleep, gmtime

# # url = "https://opensea.io/" + username
# url='https://google.com'
# headers = {
#     "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36",
# }
# proxies = {
#     "https": "https://192.187.126.98:19016",
# }

# response = requests.get(url, headers=headers, proxies=proxies, timeout=10)
# print(response)
# print(response.status_code)