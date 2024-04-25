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

instrument = '6616ae5ad12f72c95c0a7854'
instrumentId = ObjectId(instrument)
start = 1713243600
end = 1713848399


datas = db['bigdatas']
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
dbars['time_diff'] = dbars['datetime'].diff().dt.total_seconds() / 60
dbars = dbars[(dbars['color'] != '#898989') | (dbars['time_diff'] != 1)]

# dbars = dbars[['y', 'color', 'x']]


#region ANALISIS DFINAL

coordenadas = []
alarma = False
parameter = 25
for i in range(len(df_data)):
    if df_data['value'][i] >= parameter and not alarma:
        alarma = True
        coordenadas.append([df_data['datetime'][i], df_data['value'][i]])
    elif df_data['value'][i] < parameter and alarma:
        alarma = False
        coordenadas.append([df_data['datetime'][i], df_data['value'][i]])
diferencia = []
start_df = []
end_df = []
mean = 0
for i in range(0, len(coordenadas), 2):
    if i + 1 < len(coordenadas):
        s = coordenadas[i][0]
        e = coordenadas[i + 1][0]
        time_dif = e - s
        diferencia.append((time_dif))
        start_df.append(s)
        end_df.append(e)

df_final = pd.DataFrame({'start': start_df, 'end': end_df, 'duration': diferencia})

if len(df_final) > 0:
    df_final['duration'] = df_final['duration'].dt.total_seconds() / 60
    df_final['tstart'] = df_final['start'].astype('int64') / 1e6
    df_final['tend'] = df_final['end'].astype('int64') / 1e6
    df_final['day'] = df_final['start'].dt.day_name()
    df_final['day'] = df_final['day'].replace(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'], ['Lunes', 'Martes', 'Miercoles', 'Jueves', 'Viernes', 'Sabado', 'Domingo'])
    df_final = df_final.query('duration > 20')
    if len(df_final) > 0:
        df_final['timestart'] = pd.to_datetime(df_final['start'])
        df_final = df_final.reset_index(drop=True)
        mean = df_final['duration'].mean()

#region FUNCTIONS
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

def complete_daily(df):
    date_range = pd.date_range(start=df['datetime'].min().date(), end=df['datetime'].max().date(), freq='D')
    missing_rows = []
    for date in date_range:
        if date not in df['datetime'].dt.date.unique():
            missing_rows.append({
                'y': date.strftime("%Y-%m-%d"),
                'color': '#898989',
                'x': '23:59'
            })

    if missing_rows:
        missing_df = pd.DataFrame(missing_rows)
        df = pd.concat([df, missing_df])

    df = df.sort_values(by=['y', 'x']).reset_index(drop=True)
    
    return df

def complete_data(dbars, start, end):
    start_date = datetime.fromtimestamp(start)
    end_date = datetime.fromtimestamp(end)
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    modified_rows = []

    for date in date_range:
        df_date = dbars[dbars['y'] == date.strftime('%Y-%m-%d')]
        
        if date == date_range[-1] and not df_date.empty:
            continue
        
        if '23:59' not in df_date['x'].values:
            modified_rows.append({'color': '#898989', 'x': '23:59', 'y': date.strftime('%Y-%m-%d')})

    df_modified_rows = pd.DataFrame(modified_rows)
    dbars = pd.concat([dbars, df_modified_rows], ignore_index=True)
    dbars = dbars.reset_index(drop=True)
    dbars = dbars[['y', 'color', 'x']]
    
    return dbars

# dbars[dbars['x'] == '23:59']