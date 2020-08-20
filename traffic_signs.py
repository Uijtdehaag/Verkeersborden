# verkeersborden download API 
# https://docs.ndw.nu/api/trafficsigns/en/index.html 
#
# call api and save data to csv file

import requests, json
import pandas as pd
from pandas.io.json import json_normalize

pdObjNorm = None
status_code = 200

# start a offset
# default: '1970-01-01T00:00:00.000Z'
offset = '1970-01-01T00:00:00.000Z'
#offset = '2020-08-01T12:00:00.000Z'

last_offset = ''
object_count = 0

while (status_code == 200 and offset != last_offset):
    last_offset = offset
    url = 'https://data.ndw.nu/api/rest/static-road-data/traffic-signs/v1/events?offset={}&limit=100'.format(offset)
    response = requests.get(url, verify=False)
    status_code = response.status_code
    print( "{} Status:{} Objects:{}".format(url, status_code, object_count) )
    if (status_code == 200):
        dataJSON = response.json()
        object_count += len(dataJSON)
        if (pdObjNorm is None):
            pdObjNorm = json_normalize(dataJSON)
        else:
            pdObjNorm = pdObjNorm.append(json_normalize(dataJSON))
        offset = pdObjNorm['publication_timestamp'].max()
        

#df_duplicates_removed = pdObjNorm.drop_duplicates()
pdObjNorm.to_csv("traffic_signs.csv",index=False,sep=';')
