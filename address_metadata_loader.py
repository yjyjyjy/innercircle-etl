import pandas as pd
import etl_utls as utl
from address_metadata import address_metadata_worker as mw
import json
import glob
import datetime

def parse_metadata_json(data):
    meta = {}
    meta['id'] = data['address']
    meta['opensea_display_name'] = data['displayName'] or data['user']['publicUsername']
    # datetime.datetime.strptime(data['createdDate'].split('.')[0], '%Y-%m-%dT%H:%M:%S') #"createdDate": "2021-03-13T05:48:10.653999",
    meta['opensea_image_url'] = data['imageUrl']
    meta['opensea_banner_image_url'] = data['bannerImageUrl']
    meta['opensea_bio'] = data['bio']
    meta['twitter_username'] = data['metadata']['twitterUsername']
    meta['instagram_username'] = data['metadata']['instagramUsername']
    meta['website'] = data['metadata']['websiteUrl']
    meta['opensea_user_created_at'] = data['createdDate']
    meta['last_updated_at'] = datetime.datetime.now()
    return meta


# mw.ADDRESS_META_FINISHED_FILE
# for now just grab all files and do upsert
files = glob.glob('./address_metadata/metadata/*')

output = pd.DataFrame()
for file in files:

    with open(file) as json_file:
        data = json.load(json_file)

    try:
        meta = parse_metadata_json(data)
        row = pd.DataFrame(meta, index=[0])
        if output.empty:
            output = row
        else:
            output = output.append(row)
    except:
        print(f"🤯🤯 error parsing address metadata json file: {file}")

utl.copy_from_df_to_postgres(df = output, table='address_metadata_opensea', csv_filename_with_path=None, use_upsert=True, key='id')
utl.query_postgres(sql='''
    update address_metadata t
    set
        id = s.id
        , opensea_display_name = s.opensea_display_name
        , opensea_image_url = s.opensea_image_url
        , opensea_banner_image_url = s.opensea_banner_image_url
        , opensea_bio = s.opensea_bio
        , twitter_username = s.twitter_username
        , instagram_username = s.instagram_username
        , website = s.website
        , opensea_user_created_at= s.opensea_user_created_at
        , last_updated_at= s.last_updated_at
    from address_metadata_opensea s
    where s.id = t.id
    ;
''')



# read in the file
# split into addresses
# for each address, find load the corresponding json file
# compose the dataframe and load


#   "address": "0x0ccc9f78b7b7ae40f9370697f0ae26997411d398",
#   "imageUrl": "https://lh3.googleusercontent.com/FV481hRMBRA8yoswWHp0A9rDjUoQbwehFXtILkhfZcLoal46YAh1eiWylI7GBjwCbqvsTv7XTlsPc71Au-gnmqUf1w4z7aLJHK12sg",
#   "user": {
#     "username": "SunriseVentures",
#     "publicUsername": "SunriseVentures",
#     "id": "VXNlclR5cGU6MzIzOTc3",
#     "favoriteAssetCount": 84
#   },
#   "metadata": {
#     "isBanned": false,
#     "twitterUsername": null,
#     "instagramUsername": null,
#     "websiteUrl": null
#   },
#   "bio": "Unique artwork with utilizing both digital and analog medium",
#   "bannerImageUrl": null,
#   "config": null,
#   "isCompromised": false,
#   "relayId": "QWNjb3VudFR5cGU6MjM3MjUzODI=",
#   "names": [],
#   "displayName": "SunriseVentures",
#   "createdDate": "2021-03-13T05:48:10.653999",
#   "privateAssetCount": 127,
#   "id": "QWNjb3VudFR5cGU6MjM3MjUzODI="