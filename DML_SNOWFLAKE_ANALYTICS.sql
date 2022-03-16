list @gets3data

copy into eth_contracts from @gets3data
FILES=('eth_contracts.csv')
FILE_FORMAT=(FORMAT_NAME = D_FILEFORMAT);

copy into eth_transactions from @gets3data
FILES=('eth_transactions.csv')
FILE_FORMAT=(FORMAT_NAME = D_FILEFORMAT);

copy into circle from @gets3data
FILES=('circle.csv')
FILE_FORMAT=(FORMAT_NAME = D_FILEFORMAT);

copy into COLLECTION_TO_CIRCLE_MAPPING from @gets3data
FILES=('collection_to_circle_mapping.csv')
FILE_FORMAT=(FORMAT_NAME = D_FILEFORMAT);

copy into insider from @gets3data
FILES=('insider.csv')
FILE_FORMAT=(FORMAT_NAME = D_FILEFORMAT);

copy into nft_contract_floor_price from @gets3data
FILES=('nft_contract_floor_price.csv')
FILE_FORMAT=(FORMAT_NAME = D_FILEFORMAT);

copy into nft_trx_union from @gets3data
FILES=('nft_trx_union.csv')
FILE_FORMAT=(FORMAT_NAME = D_FILEFORMAT);

copy into post from @gets3data
FILES=('post.csv')
FILE_FORMAT=(FORMAT_NAME = D_FILEFORMAT);

copy into SUBSCRIBER from @gets3data
FILES=('subscriber.csv')
FILE_FORMAT=(FORMAT_NAME = D_FILEFORMAT);
