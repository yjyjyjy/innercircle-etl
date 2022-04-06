import os
import sys
import requests
import json
import logging
from logging.handlers import TimedRotatingFileHandler
from time import sleep, gmtime

# Local logging
NAME = os.path.splitext(os.path.basename(__file__))[0]
logger = logging.getLogger(NAME)
logger.setLevel(10)
format_str = "[{asctime} | " + "{levelname:^8} | " + "{name}] " + "{message}"
logging.Formatter.converter = gmtime

os.makedirs("logs", exist_ok=True)
file_handler = TimedRotatingFileHandler(filename=f"logs/{NAME}.log", when="midnight", interval=1, backupCount=30, utc=True)
file_handler.setFormatter(logging.Formatter(format_str, style="{"))
logger.addHandler(file_handler)

stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(logging.Formatter(format_str, style="{"))
logger.addHandler(stream_handler)

# Globals
base_url = "https://api.twitter.com"
headers = {
    "Authorization": "Bearer " + os.environ["TWITTER_TOKEN"],
}


def load_usernames():
    with open("twitter_usernames.txt", "r") as f:
        usernames = f.read().splitlines()
    return usernames


def get_stats(username, max_results):
    if max_results < 5 or max_results > 100:
        raise ValueError(f"max_results must be between 5 and 100, got {max_results}")

    # Get ID and user metrics
    url = f"{base_url}/2/users/by/username/{username}?user.fields=public_metrics"

    while True:
        try:
            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                break

            else:
                logger.error(f"Error: {response.status_code}")
                logger.error(response.text)
                sleep(10)

        except:
            logger.error(f"Error getting stats for {username}", exc_info=True)
            sleep(10)

    user_data = response.json()["data"]

    # Get tweet metrics
    url = f"{base_url}/2/users/{user_data['id']}/tweets?tweet.fields=public_metrics&max_results={max_results}"

    while True:
        try:
            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                break

            else:
                logger.error(f"Error: {response.status_code}")
                logger.error(response.text)
                sleep(10)

        except:
            logger.error(f"Error getting tweets for {username}", exc_info=True)
            sleep(10)

    tweets_data = response.json()["data"]

    like_count_sum = sum([tweet["public_metrics"]["like_count"] for tweet in tweets_data])
    retweet_count_sum = sum([tweet["public_metrics"]["retweet_count"] for tweet in tweets_data])
    reply_count_sum = sum([tweet["public_metrics"]["reply_count"] for tweet in tweets_data])
    quote_count_sum = sum([tweet["public_metrics"]["quote_count"] for tweet in tweets_data])

    like_count_avg = like_count_sum // len(tweets_data)
    retweet_count_avg = retweet_count_sum // len(tweets_data)
    reply_count_avg = reply_count_sum // len(tweets_data)
    quote_count_avg = quote_count_sum // len(tweets_data)

    data = user_data
    data["tweets"] = tweets_data
    data["like_count_sum"] = like_count_sum
    data["retweet_count_sum"] = retweet_count_sum
    data["reply_count_sum"] = reply_count_sum
    data["quote_count_sum"] = quote_count_sum
    data["like_count_avg"] = like_count_avg
    data["retweet_count_avg"] = retweet_count_avg
    data["reply_count_avg"] = reply_count_avg
    data["quote_count_avg"] = quote_count_avg

    return data


def save_stats(username, stats):
    os.makedirs("twitter_stats", exist_ok=True)
    with open(f"twitter_stats/{username}.json", "w") as f:
        json.dump(stats, f, indent=2)


def main():
    logger.info("Starting")
    usernames = load_usernames()
    logger.info(f"Loaded {len(usernames)} usernames")

    for username in usernames:
        logger.info(f"Getting stats for {username}")
        stats = get_stats(username, max_results=100)
        save_stats(username, stats)

    logger.info("Finished")


if __name__ == "__main__":
    try:
        main()
    except:
        logger.critical("Fatal error", exc_info=True)
