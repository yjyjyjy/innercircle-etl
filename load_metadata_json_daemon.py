import glob
import json
import os
import pandas as pd
import time
import update_etl as up
import numpy as np


LAST_UPLOADED_TIMESTAMP_FILENAME = 'last_uploaded_timestamp.json'

def get_mod_timestamp(file):
    modTimesinceEpoc = os.path.getmtime(file)
    modificationTime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(modTimesinceEpoc))
    return modificationTime


while True:
    files = glob.glob('address_metadata/metadata/*')
    df = pd.DataFrame(files, columns = ['filename'])
    df['mod_timestamp']=df.filename.apply(lambda x: get_mod_timestamp(x))
    df = df.sort_values(by='mod_timestamp')

    if len(glob.glob(LAST_UPLOADED_TIMESTAMP_FILENAME)) > 0:
        try:
            with open(LAST_UPLOADED_TIMESTAMP_FILENAME) as json_file:
                data = json.load(json_file)
            last_uploaded_timestamp = data['last_uploaded_timestamp']
            df = df[df.mod_timestamp >= last_uploaded_timestamp]
        except:
            print("🤯 error loading json file")


    batches = np.array_split(df, np.ceil(df.shape[0]/1000))
    for batch in batches:
        up.load_address_metadata_from_json(files = batch.filename.to_list())
        print(batch.mod_timestamp.max())
        data = {'last_uploaded_timestamp':batch.mod_timestamp.max()}
        with open(LAST_UPLOADED_TIMESTAMP_FILENAME, 'w') as outfile:
            json.dump(data, outfile) # data is a list of dicts

    time.sleep(60)





