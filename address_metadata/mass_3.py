import os
import sys
from weakref import proxy
import requests
from requests.exceptions import ReadTimeout, SSLError, ProxyError
from bs4 import BeautifulSoup
import json
from concurrent.futures import ThreadPoolExecutor
import logging
from logging.handlers import TimedRotatingFileHandler
from time import sleep, gmtime
import os

PROXY_LIST = [
    {"https":"37.48.118.4:13041"},
    {"https":"5.79.66.2:13041"},
    {"https":"173.208.150.242:15002"},
    {"https":"173.208.213.170:15002"},
    {"https":"173.208.239.10:15002"},
    {"https":"173.208.136.2:15002"},
    {"https":"195.154.255.118:15002"},
    {"https":"195.154.222.228:15002"},
    {"https":"195.154.255.34:15002"},
    {"https":"195.154.222.26:15002"}
    ]

script_name = os.path.basename(__file__)
script_number = script_name.split('.')[0][-1]

# cd /home/junyuyang/etl/address_metadata;/usr/bin/python3 mass_3.py
ADDRESS_META_TODO_FILE = f'todo_{script_number}.csv'
proxy = PROXY_LIST[int(script_number)]


# Local logging
NAME = os.path.splitext(os.path.basename(__file__))[0]
logger = logging.getLogger(NAME)
logger.setLevel(10)
format_str = "[{asctime} | " + "{levelname:^8} | " + "{threadName:^12} | " + "{name}] " + "{message}"
logging.Formatter.converter = gmtime

os.makedirs("logs", exist_ok=True)
file_handler = TimedRotatingFileHandler(filename=f"logs/{NAME}.log", when="midnight", interval=1, backupCount=30, utc=True)
file_handler.setFormatter(logging.Formatter(format_str, style="{"))
logger.addHandler(file_handler)

stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(logging.Formatter(format_str, style="{"))
logger.addHandler(stream_handler)


def load_usernames():

    if not os.path.isfile(ADDRESS_META_TODO_FILE):
        return []
    f = open(ADDRESS_META_TODO_FILE)
    usernames = []
    for line in f:
        usernames.append(line.rstrip())
    return usernames
    # with open(ADDRESS_META_TODO_FILE, "r") as f:
    #     usernames = f.read().splitlines()
    # return usernames



def get_metadata(username):
    logger.info(f"Getting metadata for {username}")

    url = "https://opensea.io/" + username
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36",
    }
    proxies = proxy



    retry = 10

    while retry > 0:
        try:
            response = requests.get(url, headers=headers, proxies=proxies, timeout=30)

            if response.status_code == 200:
                break

            else:
                logger.error(f"Error: {response.status_code}")
                sleep(10)

        except ReadTimeout:
            logger.error("Timeout")
            sleep(1)

        except SSLError:
            logger.error("SSL Error")
            sleep(10)

        except ProxyError:
            logger.error("Proxy error")
            sleep(1)

        except:
            logger.error(f"Error getting metadata for {username}", exc_info=True)

        finally:
            retry -= 1
            sleep(10)

    soup = BeautifulSoup(response.text, "html.parser")

    try:
        script_tag = soup.find("script", id="__NEXT_DATA__")
        json_data = json.loads(script_tag.text)
        account = json_data["props"]["relayCache"][0][1]["json"]["data"]["account"]
        save_metadata(username, account)

    except:
        logger.error(f"Error parsing metadata for {username}", exc_info=True)


def save_metadata(username, metadata):
    os.makedirs("metadata", exist_ok=True)
    with open(f"metadata/{username}.json", "w") as f:
        json.dump(metadata, f, indent=2)


def main():
    logger.info("Starting")
    usernames = load_usernames()
    logger.info(f"Loaded {len(usernames)} usernames")

    # for username in usernames:
    #     metadata = get_metadata(username)
    #     save_metadata(username, metadata)

    with ThreadPoolExecutor(max_workers=35, thread_name_prefix="Thread") as executor:
        executor.map(get_metadata, usernames)

    logger.info("Finished")


if __name__ == "__main__":
    try:
        main()
    except:
        logger.critical("Fatal error", exc_info=True)
