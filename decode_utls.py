from const import OPENSEA_TRADING_CONTRACT_V1, OPENSEA_TRADING_CONTRACT_V2
import etl_utls as utl
from eth_utils import to_hex
from functools import lru_cache
from hexbytes import HexBytes
import json
import os
import pandas as pd
import requests
import sys
from web3 import Web3
from web3.auto import w3
from web3._utils.events import get_event_data

OPENSEA_V1_ABI_FILENAME = os.environ.get("OPENSEA_V1_ABI_FILENAME")
OPENSEA_V2_ABI_FILENAME = os.environ.get("OPENSEA_V2_ABI_FILENAME")

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
    w3_endpoint = Web3(Web3.HTTPProvider(INFURA_ENDPOINT))
    address = Web3.toChecksumAddress(address)
    contract = w3_endpoint.eth.contract(address=address, abi=abi)
    return contract

# **********************************************************
# ****************** Decoding OpenSea **********************
# **********************************************************

################### trading price ##########################

# def get_opensea_contract():
#     # get abi and then contract object
#     print('📜 getting openssea contract')
#     search = glob.glob("./" + OPENSEA_V1_ABI_FILENAME)
#     if len(search) == 0:
#         abi = fetch_abi(OPENSEA_V1_ABI_FILENAME, json_file=OPENSEA_V1_ABI_FILENAME)
#     else:
#         abi = load_abi_json(OPENSEA_V1_ABI_FILENAME)

#     contract = create_contract_obj(OPENSEA_TRADING_CONTRACT_V1, abi)
#     return contract


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


################### trading price #########################
# decode the input data for opensea trades and generate buyer, seller fields
# factor the create contract part out to improve performance. It only needs to be done once.
def decode_tuple(t, target_field):
    output = dict()
    for i in range(len(t)):
        if isinstance(t[i], (bytes, bytearray)):
            output[target_field[i]['name']] = to_hex(t[i])
        elif isinstance(t[i], (tuple)):
            output[target_field[i]['name']] = decode_tuple(t[i], target_field[i]['components'])
        else:
            output[target_field[i]['name']] = t[i]
    return output

def decode_list_tuple(l, target_field):
    output = l
    for i in range(len(l)):
        output[i] = decode_tuple(l[i], target_field)
    return output

def decode_list(l):
    output = l
    for i in range(len(l)):
        if isinstance(l[i], (bytes, bytearray)):
            output[i] = to_hex(l[i])
        else:
            output[i] = l[i]
    return output

def convert_to_hex(arg, target_schema):
    """
    utility function to convert byte codes into human readable and json serializable data structures
    """
    output = dict()
    for k in arg:
        if isinstance(arg[k], (bytes, bytearray)):
            output[k] = to_hex(arg[k])
        elif isinstance(arg[k], (list)) and len(arg[k]) > 0:
            target = [a for a in target_schema if 'name' in a and a['name'] == k][0]
            if target['type'] == 'tuple[]':
                target_field = target['components']
                output[k] = decode_list_tuple(arg[k], target_field)
            else:
                output[k] = decode_list(arg[k])
        elif isinstance(arg[k], (tuple)):
            target_field = [a['components'] for a in target_schema if 'name' in a and a['name'] == k][0]
            output[k] = decode_tuple(arg[k], target_field)
        else:
            output[k] = arg[k]
    return output

@lru_cache(maxsize=None)
def _get_contract(address, abi):
    """
    This helps speed up execution of decoding across a large dataset by caching the contract object
    It assumes that we are decoding a small set, on the order of thousands, of target smart contracts
    """
    if isinstance(abi, (str)):
        abi = json.loads(abi)

    contract = w3.eth.contract(address=Web3.toChecksumAddress(address), abi=abi)
    return (contract, abi)

def decode_opensea_trade_to_extract_currency(input_data, abi, contract):
    if abi is not None:
        try:
            if isinstance(abi, (str)):
                abi = json.loads(abi)
            func_obj, func_params = contract.decode_function_input(input_data)
            target_schema = [a['inputs'] for a in abi if 'name' in a and a['name'] == func_obj.fn_name][0]
            decoded_func_params = convert_to_hex(func_params, target_schema)
            payment_token = decoded_func_params['addrs'][6].lower()
            return payment_token
        except:
            e = sys.exc_info()[0]
            return f'<error> decoding error: {repr(e)}'
    else:
        return '<error> no matching abi'

def get_opensea_trade_currency(date):
    print("🦄🦄 getting get nft trade currency from call input data: " + date)
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
    opensea_abi_v2 = load_abi_json(OPENSEA_V2_ABI_FILENAME)
    (opensea_contract_v2, _) = _get_contract(address=OPENSEA_TRADING_CONTRACT_V2, abi=opensea_abi_v2)
    mod = df.apply(
        lambda row: decode_opensea_trade_to_extract_currency(
            input_data=row.input_data
            , abi=opensea_abi_v2
            , contract=opensea_contract_v2
            ), axis = 'columns', result_type='expand')
    currency = pd.concat([df, mod], axis = 1)[['timestamp','trx_hash','eth_value', 0, 'platform']]
    currency.columns = ['timestamp','trx_hash','eth_value', 'payment_token', 'platform']
    return currency
