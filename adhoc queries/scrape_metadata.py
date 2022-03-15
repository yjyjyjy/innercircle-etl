import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
import etl_utls as utl
import pandas as pd


def wait_for_elements_to_render(browser, xpath, wait_for_upto=10):
    timer = 0
    while timer <= wait_for_upto:
        elements = browser.find_elements_by_xpath(xpath)
        if len(elements) > 0:
            return elements
        time.sleep(1)
        timer += 1
    return []


# use selenium to retrieve collection's meta data
def get_contract_meta_data_from_opensea_selenium(contract):
    collection_keys = [
        "name",
        "safelist_request_status",
        "description",
        "image_url",
        "banner_image_url",
        "external_url",
        "twitter_username",
        "discord_url",
        "telegram_url",
        "instagram_username",
        "medium_username",
        "wiki_url",
        "payout_address",
        "slug",
    ]
    meta = {"address": contract}
    for key in collection_keys:
        meta[key] = None

    # starting a headless browser
    browser = webdriver.Chrome()
    wait_upto = 10

    # navigate browser to find the collection
    browser.get("https://opensea.io")
    time.sleep(2)
    elements = wait_for_elements_to_render(browser, xpath='//input[@type="text"]')
    input_box = elements[0]
    input_box.send_keys(contract)
    input_box.send_keys(Keys.RETURN)

    cards = wait_for_elements_to_render(browser, xpath="//div[contains(@class, 'CollectionSearchCarousel--one-card')]")
    if len(cards) == 0:
        print(f"🌊🌊 failed to find collection on OpenSea via selenium: {contract}. Saving Unnamed")
        meta["name"] = "Unnamed"
        return meta

    cards[0].click()

    elements = wait_for_elements_to_render(browser, xpath="//h1")
    if len(elements) == 0:
        print(f"🤯🤯 🌊🌊 Found the collection on OpenSea but cannot find name via h1 tag: {contract}. Saving Unnamed")
        meta["name"] = "Unnamed"
        return meta

    element = elements[0]
    meta["name"] = element.text
    meta["slug"] = browser.current_url.split("opensea.io/collection/")[-1]

    # images
    el = browser.find_elements_by_xpath("//img[@alt='Banner Image']")
    if len(el) > 0:
        meta["banner_image_url"] = el[0].get_attribute("src")
    el = browser.find_elements_by_xpath("//div[contains(@class, 'CollectionHeader--collection-image')]//img")
    if len(el) > 0:
        meta["image_url"] = el[0].get_attribute("src")

    # description
    p_eles = browser.find_elements_by_xpath("//div[@class='CollectionHeader--description']//p")
    meta["description"] = "\n".join([p.text for p in p_eles])

    # social and website
    social_buttons = browser.find_elements_by_xpath(
        "//a[contains(@class, 'ButtonGroupreact__StyledButton-sc-1skvztv-0')]"
    )
    connected_social_pills = browser.find_elements_by_xpath(
        "//div[contains(@class, 'CollectionHeader--ConnectedSocials--pill')]/ancestor::a"
    )
    social_buttons += connected_social_pills
    links = [button.get_attribute("href") for button in social_buttons]
    for link in links:
        if "instagram.com/" in link:
            meta["instagram_username"] = link.split("instagram.com/")[-1]
        elif "discord.gg/" in link:
            meta["discord_url"] = link
        elif "https://twitter.com/" in link:
            meta["twitter_username"] = link.split("https://twitter.com/")[-1]
        elif "/t.me/" in link:
            meta["telegram_url"] = link
        elif "medium.com/@" in link:
            meta["medium_username"] = link.split("medium.com/@")[-1]
        else:
            meta["external_url"] = link

    browser.close()
    return meta


# update the nft contract meta data by calling the OpenSea single asset api https://docs.opensea.io/reference/retrieving-a-single-asset
def update_collection(pagination=5):
    result = utl.query_postgres(sql="select address from new_nft_contracts where missing_metadata", columns=["address"])
    todo_list = result.address.to_list()

    # output schema
    """
        "address",
        "name",
        "safelist_request_status",
        "description",
        "image_url",
        "banner_image_url",
        "external_url",
        "twitter_username",
        "discord_url",
        "telegram_url",
        "instagram_username",
        "medium_username",
        "wiki_url",
        "payout_address",
        "slug"
    """

    wait_time = 1.5
    while len(todo_list) > 0:
        print(f"🦾🦾 todo list len : {len(todo_list)}")
        _todo = todo_list[:pagination]
        output = pd.DataFrame()

        for contract in _todo:
            # try:
            #     meta, status_code = utl.get_contract_meta_data_from_opensea(contract)
            # except Exception as e:
            #     print("🤯 Error decoding contract meta data", e)
            #     if status_code != None:
            #         print("status_code = " + status_code)
            #     continue

            # if status_code in [429, 404]:
            #     print(f"⏱ current wait_time: {wait_time}")
            #     time.sleep(60)
            #     if wait_time <= 5:
            #         wait_time += 0.5

            meta = get_contract_meta_data_from_opensea_selenium(contract)
            row = pd.DataFrame(meta, index=[0])

            if output.empty:
                output = row
            else:
                output = output.append(row)
            time.sleep(wait_time)

        print("🧪🧪🧪 upserting output data")
        print(output[["address", "name"]])

        if output.shape[0] > 0:
            utl.copy_from_df_to_postgres(df=output, table="collection", use_upsert=True, key="id")
        todo_list = [x for x in todo_list if x not in _todo]
