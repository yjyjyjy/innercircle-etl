import etl_utls as utl
from functools import lru_cache
from web3._utils.events import get_event_data
from hexbytes import HexBytes
import json
import pandas as pd
from web3 import Web3
infura_endpoint = "https://mainnet.infura.io/v3/282f04d814ba4cddb58390c0a41a97f2"
w3 = Web3(Web3.HTTPProvider(infura_endpoint))

def decode_opensea_trade_log_to_extract_price(data, topics):
    abi = utl.load_abi_json('opensea_v1_abi.json')
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
