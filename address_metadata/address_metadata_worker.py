import os
import sys
import requests
from requests.exceptions import ReadTimeout, SSLError, ProxyError
from bs4 import BeautifulSoup
import json
from concurrent.futures import ThreadPoolExecutor
import logging
from logging.handlers import TimedRotatingFileHandler
from time import sleep, gmtime
import os

ADDRESS_META_TODO_FILE = 'addresses_todo.csv'
ADDRESS_META_FINISHED_FILE = 'addresses_finished.csv'
PROXY_LIST = [
    {"https": "https://192.187.126.98:19016"},
    {"https": "https://142.54.160.122:19006"},
    {"https": "https://192.187.125.234:19001"},
    {"https": "https://192.187.111.82:17007"},
    {"https": "https://192.187.125.234:19008"}
]

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

def rename_todo_file():
    if os.path.isfile(ADDRESS_META_TODO_FILE):
        os.rename(ADDRESS_META_TODO_FILE, ADDRESS_META_FINISHED_FILE)



def get_metadata(username):
    logger.info(f"Getting metadata for {username}")

    url = "https://opensea.io/" + username
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36",
    }
    proxies = {
        "https": "https://192.187.126.98:19016",
    }



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

    with ThreadPoolExecutor(max_workers=50, thread_name_prefix="Thread") as executor:
        executor.map(get_metadata, usernames)


    rename_todo_file()

    logger.info("Finished")


if __name__ == "__main__":
    try:
        main()
    except:
        logger.critical("Fatal error", exc_info=True)
