# -*- coding: utf-8 -*-
"""
Created on Tue Jan 28 09:45:47 2020

@author: chauman.fung@glasgow.ac.uk
"""
# details of GBFS: https://github.com/NABSA/gbfs/blob/master/gbfs.md#free_bike_statusjson
# Next Bike
# https://api.nextbike.net/maps/gbfs/v1/nextbike_gg/gbfs.json This is the link of api in the email. 
# For the other bike share companies, just click on "real time/ live GBFS feed" for all the links with available info.

# What I found for next bike:
#{"name":"system_information","url":"https://gbfs.nextbike.net/maps/gbfs/v1/nextbike_gg/en/system_information.json"},
#{"name":"station_information","url":"https://gbfs.nextbike.net/maps/gbfs/v1/nextbike_gg/en/station_information.json"}
#{"name":"station_status","url":"https://gbfs.nextbike.net/maps/gbfs/v1/nextbike_gg/en/station_status.json"}
#{"name":"free_bike_status","url":"https://gbfs.nextbike.net/maps/gbfs/v1/nextbike_gg/en/free_bike_status.json"}
#{"name":"system_hours","url":"https://gbfs.nextbike.net/maps/gbfs/v1/nextbike_gg/en/system_hours.json"}
#{"name":"system_regions","url":"https://gbfs.nextbike.net/maps/gbfs/v1/nextbike_gg/en/system_regions.json"}

import pandas as pd
import requests
from pandas.io.json import json_normalize
from sqlalchemy import create_engine
import psycopg2
import io
import schedule
import datetime
import time

url="https://gbfs.nextbike.net/maps/gbfs/v1/nextbike_gg/en/station_status.json"

# timstamp "last_reported": the number of seconds since January 1st 1970 00:00:00 UTC 
# (convertor: https://www.unixtimestamp.com/index.php)
def job():
    try:    
        content = requests.get(url).json()  
        content1 = json_normalize(content['data']['stations'])             
        engine = create_engine('postgresql+psycopg2://postgres:ubdctransport@127.0.0.1:5432/postgres') #USERNAME:PASSWORD@ADDRESS:PORT/NAMEDB
        content1.head(0).to_sql('nextbike', engine,if_exists='append',index=False) 
        conn = engine.raw_connection()
        cur = conn.cursor()
        output = io.StringIO()
        content1.to_csv(output, sep='\t',header=False,index=False)
        output.seek(0)
        cur.copy_from(output,'nextbike', null="") # null values become ''
        conn.commit()
        cur.close()
        conn.close()
        print(" Successfully written to database ")
        now = datetime.datetime.now()
        print ("Current date and time : ")
        print (now.strftime("%Y-%m-%d %H:%M:%S"))

    except (Exception, psycopg2.DatabaseError) as error :
        print ("Error in writing to database:", error)
        
# according to https://gbfs.nextbike.net/maps/gbfs/v1/nextbike_gg/en/system_information.json 
# ttl: Number of seconds before the data in the feed will be updated again (0 if the data should always be refreshed).
# updated every 60 seconds
# but it hasn't been updated since 08:45, 28 Jan. The time now is 11:15, 28 Jan. Need to confirm how often this is updated.
        
# schedule this task every 15 minutes 
schedule.every(15).minutes.do(job)

while True:
    schedule.run_pending()
    time.sleep(1)            