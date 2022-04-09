import os
import sys
import requests
import json
import logging
from logging.handlers import TimedRotatingFileHandler
import time
from time import sleep, gmtime
import etl_utls as utl
import pandas as pd
import glob

TWITTER_TOKEN='AAAAAAAAAAAAAAAAAAAAACbSbAEAAAAA%2Fq2ryf4d9Vdvj1ugpR%2FEZ1S5jhQ%3DBmjc6jJjrpExHyAY35TRQkl21SYH03HxuxALcuJLGz2G0gLqW5'

df = utl.query_postgres(sql = 'select twitter_username from tmp_export;', columns=['tw'])
usernames = df.tw.to_list()
existing = [e.split('/')[-1].replace('.json', '') for e in glob.glob('twitter_info/*')]
usernames = [name for name in usernames if name not in existing]


# Globals
base_url = "https://api.twitter.com"
headers = {
    "Authorization": "Bearer " + TWITTER_TOKEN,
}


def flatten_data(y):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out


def get_stats(username):

    # Get ID and user metrics
    url = f"{base_url}/2/users/by/username/{username}?user.fields=public_metrics"
    row = pd.DataFrame()
    # try:
    response = requests.get(url, headers=headers)

    if response.status_code == 200:

        # dumping data into json first
        with open(f'twitter_info/{username}.json', 'w') as outfile:
            json.dump(response.json(), outfile)

        user_data = response.json().get("data")
        if user_data != None:
            row = pd.DataFrame(flatten_data(user_data), index=[0])

    elif response.status_code == 429:
        cool_down = int(response.headers['x-rate-limit-reset']) - int(time.time())+1
        print('cooling down: ' +str(cool_down))
        sleep(cool_down)

    else:
        print('bad username: ' + username)
        print(response.status_code)

    # except:
    #     print('bad username: ' + username)

    return row

def main():
    output = pd.DataFrame()
    counter = 0
    for username in usernames:
        row = get_stats(username)
        if not row.empty:
            if output.empty:
                output = row
            else:
                output = output.append(row)

        sleep(1)

        counter += 1
        if counter %1000 == 0:
            print(f'progress: {str(counter)}/{str(len(usernames))}')
            output.to_csv('twitter_output.csv', index=False)



# def save_stats(username, stats):
#     os.makedirs("twitter_stats", exist_ok=True)
#     with open(f"twitter_stats/{username}.json", "w") as f:
#         json.dump(stats, f, indent=2)


# def main():
#     logger.info("Starting")
#     usernames = load_usernames()
#     logger.info(f"Loaded {len(usernames)} usernames")

#     for username in usernames:
#         logger.info(f"Getting stats for {username}")
#         stats = get_stats(username, max_results=100)
#         save_stats(username, stats)

#     logger.info("Finished")


if __name__ == "__main__":
    main()
