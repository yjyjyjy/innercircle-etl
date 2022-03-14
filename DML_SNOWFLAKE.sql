list @gets3data

copy into eth_contracts from @gets3data
FILES=('eth_contracts.csv')
FILE_FORMAT=(FORMAT_NAME = D_FILEFORMAT);

copy into "JUNYU_DB"."DTEST"."eth_transactions" from @gets3data
FILES=('eth_transactions.csv')
FILE_FORMAT=(FORMAT_NAME = D_FILEFORMAT);

select count(*) from "JUNYU_DB"."DTEST"."eth_transactions";

copy into "JUNYU_DB"."DTEST"."circle" from @gets3data
FILES=('circle.csv')
FILE_FORMAT=(FORMAT_NAME = D_FILEFORMAT);

copy into "JUNYU_DB"."DTEST"."COLLECTION_TO_CIRCLE_MAPPING" from @gets3data
FILES=('collection_to_circle_mapping.csv')
FILE_FORMAT=(FORMAT_NAME = D_FILEFORMAT);

copy into "JUNYU_DB"."DTEST"."insider" from @gets3data
FILES=('insider.csv')
FILE_FORMAT=(FORMAT_NAME = D_FILEFORMAT);

copy into "JUNYU_DB"."DTEST"."nft_contract_floor_price" from @gets3data
FILES=('nft_contract_floor_price.csv')
FILE_FORMAT=(FORMAT_NAME = D_FILEFORMAT);

//copy into "JUNYU_DB"."DTEST"."" from @gets3data
//FILES=('nft_trx_union.csv')
//FILE_FORMAT=(FORMAT_NAME = D_FILEFORMAT);

copy into "JUNYU_DB"."DTEST"."post" from @gets3data
FILES=('post.csv')
FILE_FORMAT=(FORMAT_NAME = D_FILEFORMAT);

copy into "JUNYU_DB"."DTEST"."SUBSCRIBER" from @gets3data
FILES=('subscriber.csv')
FILE_FORMAT=(FORMAT_NAME = D_FILEFORMAT);
