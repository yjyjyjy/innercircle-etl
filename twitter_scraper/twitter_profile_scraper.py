import os
import sys
import logging
from logging.handlers import TimedRotatingFileHandler
from time import gmtime
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import json

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

os.makedirs("twitter_usernames", exist_ok=True)


def start_driver():
    logger.info("Starting driver")

    options = webdriver.ChromeOptions()
    # Run headless
    options.add_argument("--headless")
    # No sandbox
    options.add_argument("--no-sandbox")
    # Disable /dev/shm usage
    options.add_argument("--disable-dev-shm-usage")
    # Disable images
    options.add_experimental_option("prefs", {"profile.managed_default_content_settings.images": 2})
    # Add user agent
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.60 Safari/537.36")
    driver = webdriver.Chrome(options=options)
    return driver


def load_usernames():
    with open("twitter_todo.csv", "r") as f:
        usernames = f.read().splitlines()
    return usernames


def load_profile(driver, username):
    logger.info(username)

    data = {
        "valid": None,
        "followers": None,
    }

    driver.get(f"https://twitter.com/{username}")

    for _ in range(5):
        try:
            try:
                WebDriverWait(driver, 1).until(EC.presence_of_element_located((By.XPATH, f"//div[contains(@aria-label, 'Follow ')]")))
                logger.info(f"{username} | VALID")
                data["valid"] = True

                try:
                    followers_tag = driver.find_element(By.XPATH, f"//a[contains(@href, '/followers')]")
                    followers_count = followers_tag.find_element(By.XPATH, "./*").text
                    logger.info(f"{username} | {followers_count} followers")
                    data["followers"] = followers_count

                except:
                    logger.error(f"Error parsing followers count for {username}", exc_info=True)

                break

            except TimeoutException:
                logger.warning(f"Timeout finding the follow button for {username}")

            try:
                WebDriverWait(driver, 1).until(EC.presence_of_element_located((By.XPATH, "//span[text()='This account doesn’t exist' or text()='Account suspended']")))
                logger.info(f"{username} | ERROR")
                data["valid"] = False
                break

            except TimeoutException:
                logger.warning(f"Timeout finding the account doesn't exist message for {username}")

        except:
            logger.error(f"Error parsing profile for {username}", exc_info=True)
            data["valid"] = False

    else:
        logger.error(f"Couldn't find follow button or error message for {username}, skipping")
        data["valid"] = False

    with open(f"twitter_usernames/{username}.json", "w") as f:
        json.dump(data, f)


def main():
    logger.info("Starting")

    driver = start_driver()

    usernames = load_usernames()
    logger.info(f"Loaded {len(usernames)} usernames")

    for username in usernames:
        load_profile(driver, username)

    driver.quit()

    logger.info("Finished")


if __name__ == "__main__":
    main()
