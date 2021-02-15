# verkeersborden download API 
# https://docs.ndw.nu/api/trafficsigns/en/index.html 
#
# call api and save data to csv file
import requests, json, os
import pandas as pd
from pandas.io.json import json_normalize

def GetLastOffset(filename):
        
    # Open ID file
    if os.path.isfile(filename):
        idFile = open(filename, "r")    
        Offset = idFile.read()        
        idFile.close()
    else:
        # default: '1970-01-01T00:00:00.000Z'
        Offset = '1970-01-01T00:00:00.000Z'
        
    return Offset

def SaveLastOffset( filename, Offset):    

    idFile = open(filename, "w")    
    idFile.write("{}".format(Offset))
    idFile.close()

pdObjNorm = None
status_code = 200

exportFileName = "traffic_signs.csv"
StoreOffSetFilename = "offset.txt"
offset = GetLastOffset(StoreOffSetFilename)

last_offset = ''
object_count = 0

while (status_code == 200 and offset != last_offset):
    last_offset = offset
    
    #filter for roosendaal (GM1674)
    towncode='GM1674'
    url = 'https://data.ndw.nu/api/rest/static-road-data/traffic-signs/v1/events?offset={}&limit=100&town-code={}'.format(offset,towncode)
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
                     
        if os.path.isfile(exportFileName):
            pdObjNorm.to_csv(exportFileName,index=False,sep=';', mode='a', header=False)
        else:
            pdObjNorm.to_csv(exportFileName,index=False,sep=';', mode='w', header=True)

        SaveLastOffset(StoreOffSetFilename, offset)


