import etl_utls as utl
import numpy as np
df = utl.query_postgres(sql='''
    select
        wallet_address
    from export_top_1k_vol_nft_collectors
    where crawled = false
    ;
    ''', columns=['id'])

counter = 0
date_list = np.array_split(df, 10)
for l in date_list:
    l.to_csv(f'address_metadata/todo_{str(counter)}.csv', index=False, header=False)
    counter += 1