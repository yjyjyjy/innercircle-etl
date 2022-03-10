import etl_utls as utl
from web3._utils.events import get_event_data
from hexbytes import HexBytes
import json
from web3 import Web3
import os
import glob
import requests
import pandas as pd
infura_endpoint = os.environ.get("INFURA_ENDPOINT")
w3 = Web3(Web3.HTTPProvider(infura_endpoint))
OPENSEA_V1_ABI_FILENAME = os.environ.get("OPENSEA_V1_ABI_FILENAME")
from const import OPENSEA_TRADING_CONTRACT_V1, OPENSEA_TRADING_CONTRACT_V2

# **********************************************************
# ****************** ABI and contract obj ******************
# **********************************************************

# Get ABI for smart contract NOTE: Use "to" address as smart contract 'interacted with'
# save_filename: example "abi.json"
def fetch_abi(address, json_file=None):
    address = address.lower()
    ETHERSCAN_API_KEY = os.environ.get("ETHERSCAN_API_KEY")
    abi_endpoint = (
        f"https://api.etherscan.io/api?module=contract&action=getabi&address={address}&apikey={ETHERSCAN_API_KEY}"
    )
    abi_json = json.loads(requests.get(abi_endpoint).text)
    if json_file != None:
        with open(json_file, "w") as outfile:
            json.dump(abi_json, outfile)
    return abi_json["result"]


def load_abi_json(filename):
    with open(filename) as json_file:
        abi_json = json.load(json_file)
    return abi_json["result"]


def create_contract_obj(address, abi):
    INFURA_ENDPOINT = os.environ.get("INFURA_ENDPOINT")
    w3 = Web3(Web3.HTTPProvider(INFURA_ENDPOINT))
    address = Web3.toChecksumAddress(address)
    contract = w3.eth.contract(address=address, abi=abi)
    return contract

# **********************************************************
# ****************** Decoding OpenSea **********************
# **********************************************************


def get_opensea_contract():
    # get abi and then contract object
    search = glob.glob("./" + OPENSEA_V1_ABI_FILENAME)
    if len(search) == 0:
        abi = fetch_abi(OPENSEA_V1_ABI_FILENAME, json_file=OPENSEA_V1_ABI_FILENAME)
    else:
        abi = load_abi_json(OPENSEA_V1_ABI_FILENAME)

    contract = create_contract_obj(OPENSEA_TRADING_CONTRACT_V1, abi)
    return contract


def decode_opensea_trade_log_to_extract_price(data, topics):
    abi = load_abi_json(OPENSEA_V1_ABI_FILENAME)
    if isinstance(abi, (str)):
        abi = json.loads(abi)
    hex_topics = [HexBytes(_) for _ in topics]
    log_entry = {
        'address': None, #Web3.toChecksumAddress(address),
        'blockHash': None, #HexBytes(blockHash),
        'blockNumber': None,
        'data': data,
        'logIndex': None,
        'topics': hex_topics,
        'transactionHash': None, #HexBytes(transactionHash),
        'transactionIndex': None
    }
    event_abi = {'anonymous': False,
          'inputs': [{'indexed': False, 'name': 'buyHash', 'type': 'bytes32'},
           {'indexed': False, 'name': 'sellHash', 'type': 'bytes32'},
           {'indexed': True, 'name': 'maker', 'type': 'address'},
           {'indexed': True, 'name': 'taker', 'type': 'address'},
           {'indexed': False, 'name': 'price', 'type': 'uint256'},
           {'indexed': True, 'name': 'metadata', 'type': 'bytes32'}],
          'name': 'OrdersMatched',
          'type': 'event'}

    # Convert raw JSON-RPC log result to human readable event by using ABI
    decoded_event = get_event_data(w3.codec, event_abi, log_entry)

    return decoded_event['args']['price']/10**18

# decode the input data for opensea trades and generate buyer, seller fields
# factor the create contract part out to improve performance. It only needs to be done once.
def decode_opensea_trade_to_extract_currency(data, contract = None):
    if contract == None:
        contract = get_opensea_contract()

    _, func_params = contract.decode_function_input(data)
    # for key in list(func_params.keys()):
    #     if key.startswith("calldata"):
    #         token_id = int.from_bytes(func_params[key][69 : 69 + 32], "big")
    if "addrs" in list(func_params.keys()):
        # packet = {
        #     # "nft_contract": func_params["addrs"][4].lower(),
        #     # "token_id": token_id,
        #     # "buyer": func_params["addrs"][1].lower(),
        #     # "seller": func_params["addrs"][2].lower(),

        # }
        payment_token = func_params["addrs"][6].lower()
        return payment_token

def get_opensea_trade_price(date):
    print("🦄🦄 getting get nft trade price from eth trx log: " + date)
    sql = f'''
    SELECT
        transaction_hash as trx_hash
        , data
        , topics
    FROM `bigquery-public-data.crypto_ethereum.logs`
    WHERE DATE(block_timestamp) = '{date}'
        and address in ('{OPENSEA_TRADING_CONTRACT_V1}' -- OpenSea: Wyvern Exchange v1
            , '{OPENSEA_TRADING_CONTRACT_V2}' -- OpenSea: Wyvern Exchange v2
            )
        and topics[ORDINAL(1)] like '0xc4109843%' -- OrdersMatched event
    ;
    '''
    df = utl.download_from_google_bigquery(sql)
    mod = df.apply(lambda row: decode_opensea_trade_log_to_extract_price(row.data, row.topics)
        , axis = 'columns', result_type='expand')
    price = pd.concat([df, mod], axis = 1)[['trx_hash', 0]]
    price.columns = ['trx_hash', 'price']
    price = price.groupby('trx_hash')['price'].sum().reset_index()
    return price

def get_opensea_trade_currency(date):
    print("🦄🦄 getting get nft trade currency fro: " + date)
    sql = f'''
        select
            block_timestamp as `timestamp`
            , trx.`hash` as trx_hash
            , value/pow(10,18) as eth_value
            , case when to_address = '{OPENSEA_TRADING_CONTRACT_V1}' then 'opensea v1'
                when to_address = '{OPENSEA_TRADING_CONTRACT_V2}' then 'opensea v2'
                end as platform
            , input as input_data
        from `bigquery-public-data.crypto_ethereum.transactions` trx
        where date(block_timestamp) = date('{date}')
            and to_address in ('{OPENSEA_TRADING_CONTRACT_V1}' -- OpenSea: Wyvern Exchange v1
                , '{OPENSEA_TRADING_CONTRACT_V2}' -- OpenSea: Wyvern Exchange v2
            )
            and input like '0xab834bab%' -- atomicMatch_
            and receipt_status = 1
    ;
    '''
    df = utl.download_from_google_bigquery(sql)
    contract = get_opensea_contract()
    mod = df.apply(lambda row: decode_opensea_trade_to_extract_currency(row.input_data, contract), axis = 'columns', result_type='expand')
    currency = pd.concat([df, mod], axis = 1)[['timestamp','trx_hash','eth_value', 0, 'platform']]
    currency.columns = ['timestamp','trx_hash','eth_value', 'payment_token', 'platform']
    return currency