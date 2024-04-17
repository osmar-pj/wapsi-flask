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


# CONTROLLERS
controller = db['controllers']
df_controller = pd.DataFrame(list(controller.find()))
df_controller.rename(columns={'_id': 'controllerId'}, inplace=True)

# BIGDATAS

instrumentId = '6616ae5ad12f72c95c0a7854'
instrument = ObjectId(instrumentId)

# INSTRUMENTS
instruments = db['instruments']
df_instruments = pd.DataFrame(list(instruments.find({'instrumentId': instrument })))

start = 1712725200
end = 1713329999

date_start = datetime.fromtimestamp(start)
date_end = datetime.fromtimestamp(end)
start_tz = pytz.timezone('America/Lima').localize(date_start)
end_tz = pytz.timezone('America/Lima').localize(date_end)

datas = db['bigdatas']
df_data = pd.DataFrame(list(datas.find({'instrumentId': instrument, 'ts': {'$gte': start, '$lte': end}})))
# df_data = pd.DataFrame(list(data.find()))
# df_data = pd.DataFrame(list(data.find({'createdAt': {'$gte': start_tz, '$lte': end_tz}})))
# df_data = pd.DataFrame(list(data.find({'ts': {'$gte': start, '$lte': end}})))
# df_data = df_data.merge(df_controller[['serie', 'controllerId']], on=['controllerId'], how='left')

# #region ANALISIS BARS

# serie = 'WAPSI-H1'
# name = 'CO'

# df_data = df_data.query('serie == @serie')
# df_data = df_data.query('name == @name')

df_data['datetime'] = pd.to_datetime(df_data['ts'], unit='s')
df_data['datetime'] = df_data['datetime'].dt.tz_localize('UTC').dt.tz_convert('America/Lima')

dbars = df_data[['datetime', 'color']].copy()
dbars = dbars.resample('1min',on='datetime').last().reset_index()
dbars['color'] = dbars['color'].fillna('#898989')
dbars['diff'] = dbars['datetime'].diff().dt.total_seconds().div(60).fillna(0)
dbars['group'] = (dbars['color'] != dbars['color'].shift()).cumsum()

dbars = dbars.groupby(['group', 'color']).agg({'diff': 'sum', 'datetime': 'min'}).reset_index()
dbars = dbars.query('diff >= 120 or color != "#898989"')
dbars['x'] = dbars['datetime'].dt.strftime('%H:%M')
dbars['y'] = dbars['datetime'].dt.strftime('%Y-%m-%d')
dbars = dbars.drop(['group', 'diff'], axis=1)
dbars = dbars.reset_index(drop=True)
dbars['group'] = (dbars['color'] != dbars['color'].shift()).cumsum()
dbars = dbars.groupby(['group', 'color']).agg({'datetime': 'min'}).reset_index()
dbars['x'] = dbars['datetime'].dt.strftime('%H:%M')
dbars['y'] = dbars['datetime'].dt.strftime('%Y-%m-%d')
dbars = dbars[['x', 'y', 'color']]
dbars['color_diff'] = dbars['color'].ne(dbars['color'].shift())
dbars['color_diff'] = dbars['color_diff'].cumsum()
dbars = dbars.groupby(['y', 'color_diff', 'color']).last().reset_index()
dbars = dbars.drop(['color_diff'], axis=1)


#########################
# dbars['datetime'] = dbars['y'] + ' ' + dbars['x']
# dbars['datetime'] = pd.to_datetime(dbars['datetime'])
# dbars = dbars.drop(['x', 'y'], axis=1)
# dbars = dbars.rename(columns={'datetime': 'datetime'})

#region TEST

# FUNCTION TO COMPLETE DAYS INCOMPLETE
def complete_missing_days(dbars, start, end):
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

# dbars_completed = complete_missing_days(dbars, start, end)

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

# dbars_completed = complete_empty(start, end)