from flask import Flask, jsonify, request, Response
from flask_pymongo import PyMongo
from datetime import datetime
from pymongo import MongoClient
from datetime import datetime, timedelta
from bson.objectid import ObjectId
import pandas as pd
import numpy as np
import os
import pytz

from dotenv import load_dotenv
load_dotenv()

client = MongoClient(os.getenv('MONGO_URI'))
db = client['wapsi']

pd.set_option('display.max_columns', None)

#region DATA

instrument = '6616b0f4d12f72c95c0a7e39'
instrumentId = ObjectId(instrument)
start = 1713243600
end = 1713848399


datas = db['bigdatas']
# df_data = pd.DataFrame(list(datas.find({'ts': {'$gte': start, '$lte': end}})))
df_data = pd.DataFrame(list(datas.find({'instrumentId': instrumentId, 'ts': {'$gte': start, '$lte': end}})))

#region ANALISIS BARS ONE

df_data['datetime'] = pd.to_datetime(df_data['ts'], unit='s')
df_data['datetime'] = df_data['datetime'].dt.tz_localize('UTC').dt.tz_convert('America/Lima')

dbars = df_data[["datetime", "color"]].copy()

dbars = dbars.resample('1min',on='datetime').last().reset_index()
dbars['color'] = dbars['color'].fillna('#898989')

dbars['x'] = dbars['datetime'].dt.strftime("%H:%M")
dbars['y'] = dbars['datetime'].dt.strftime("%Y-%m-%d")
dbars['color_diff'] = dbars['color'].ne(dbars['color'].shift())
dbars['color_diff'] = dbars['color_diff'].cumsum()
dbars = dbars.groupby(['y', 'color_diff', 'color']).last().reset_index()
dbars = dbars[['y', 'color', 'x']]

#region ANALISIS BARS TWO

# df_data['datetime'] = pd.to_datetime(df_data['ts'], unit='s')
# df_data['datetime'] = df_data['datetime'].dt.tz_localize('UTC').dt.tz_convert('America/Lima')

# dbars = df_data[['datetime', 'color']].copy()
# dbars = dbars.resample('1min',on='datetime').last().reset_index()
# dbars['color'] = dbars['color'].fillna('#898989')
# dbars['diff'] = dbars['datetime'].diff().dt.total_seconds().div(60).fillna(0)
# dbars['group'] = (dbars['color'] != dbars['color'].shift()).cumsum()

# dbars = dbars.groupby(['group', 'color']).agg({'diff': 'sum', 'datetime': 'min'}).reset_index()
# dbars = dbars.query('diff >= 120 or color != "#898989"')
# dbars['x'] = dbars['datetime'].dt.strftime('%H:%M')
# dbars['y'] = dbars['datetime'].dt.strftime('%Y-%m-%d')
# dbars = dbars.drop(['group', 'diff'], axis=1)
# dbars = dbars.reset_index(drop=True)
# dbars['group'] = (dbars['color'] != dbars['color'].shift()).cumsum()
# dbars = dbars.groupby(['group', 'color']).agg({'datetime': 'min'}).reset_index()
# dbars['x'] = dbars['datetime'].dt.strftime('%H:%M')
# dbars['y'] = dbars['datetime'].dt.strftime('%Y-%m-%d')
# dbars = dbars[['x', 'y', 'color']]
# dbars['color_diff'] = dbars['color'].ne(dbars['color'].shift())
# dbars['color_diff'] = dbars['color_diff'].cumsum()
# dbars = dbars.groupby(['y', 'color_diff', 'color']).last().reset_index()
# dbars = dbars.drop(['color_diff'], axis=1)

#region FUNCTIONS

# FUNCTION TO COMPLETE DAYS INCOMPLETE
def complete_days(dbars, start, end):
    start_date = datetime.fromtimestamp(start)
    end_date = datetime.fromtimestamp(end)
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    missing_days = pd.DataFrame({'datetime': date_range})
    missing_days['y'] = missing_days['datetime'].dt.strftime('%Y-%m-%d')
    missing_days = missing_days[~missing_days['y'].isin(dbars['y'])]
    missing_days['x'] = '23:59'
    missing_days['color'] = '#898989'
    dbars = pd.concat([dbars, missing_days[['x', 'y', 'color']]], ignore_index=True)
    dbars = dbars.sort_values(by=['y', 'x']).reset_index(drop=True)
    return dbars

# FUNCTION TO COMPLETE EMPTY DATA
def complete_empty(start, end):
    start_date = datetime.fromtimestamp(start)
    end_date = datetime.fromtimestamp(end)
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    missing_days = pd.DataFrame({'datetime': date_range})
    missing_days['y'] = missing_days['datetime'].dt.strftime('%Y-%m-%d')
    missing_days['x'] = '23:59'
    missing_days['color'] = '#898989'
    return missing_days[['y', 'color', 'x']]