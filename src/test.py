from flask import Flask, jsonify, request, Response
from flask_pymongo import PyMongo
from datetime import datetime
from pymongo import MongoClient
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import os
import pytz

from dotenv import load_dotenv
load_dotenv()

client = MongoClient(os.getenv('MONGO_URI'))
db = client['wapsi']

pd.set_option('display.max_columns', None)

# USUARIOS

users = db['users']
df_users = pd.DataFrame(list(users.find()))

# DATA

datas = db['datas']
bigdata = db['bigdatas']

# VETA VERITO CH738-41
start = 1711429200000
end = 1712001131803
start_seconds = int(start) / 1000
end_seconds = int(end) / 1000
instrumentId = '66032cbb6918c9ebc1cae897'
df = pd.DataFrame(list(bigdata.find({'instrumentId': instrumentId})))
## POR SEMANA


date_start = datetime.fromtimestamp(start_seconds)
date_end = datetime.fromtimestamp(end_seconds)
start_tz = pytz.timezone('America/Lima').localize(date_start)
end_tz = pytz.timezone('America/Lima').localize(date_end)
## POR MES

nro_month = '1-2024'

month, year = map(int, nro_month.split('-'))
start_date = datetime(year, month, 1)
end_date = datetime(year, month % 12 + 1, 1) if month < 12 else datetime(year + 1, 1, 1)
end_date -= timedelta(seconds=1)
end_date = end_date.replace(hour=23, minute=59, second=59)
start_tz = pytz.timezone('America/Lima').localize(start_date)
end_tz = pytz.timezone('America/Lima').localize(end_date)
df_datas = pd.DataFrame(list(datas.find({'serie': serie, 'name': name, 'createdAt': {'$gte': start_tz, '$lte': end_tz}})))

## RESTO

df_devices = pd.DataFrame(list(df_datas['devices']))
df_devices['timestamp'] = df_datas['timestamp']
df_devices['datetime'] = pd.to_datetime(df_devices['timestamp'], unit='ms')
df_devices['datetime'] = df_devices['datetime'].dt.tz_localize('UTC').dt.tz_convert('America/Lima')
df_devices['color'] = ['#0BB97D' if value < 25 else ('#B9A50C' if 25 <= value <= 50 else '#B90C26') for value in df_devices['value']]

dbars = df_devices[["datetime", "color"]].copy()
dbars['x'] = dbars['datetime'].dt.strftime("%H:%M")
dbars['y'] = dbars['datetime'].dt.strftime("%Y-%m-%d")
dbars.drop('datetime', axis = 1)
dbars = dbars[["x", "y", "color"]]
dbars['color_diff'] = dbars['color'].ne(dbars['color'].shift())
dbars['color_diff'] = dbars['color_diff'].cumsum()
dbars = dbars.groupby(['y', 'color_diff', 'color']).last().reset_index()

coordenadas = []
alarma = False
parameter = 25
for i in range(len(df_devices)):
    if df_devices['value'][i] >= parameter and not alarma:
        alarma = True
        coordenadas.append([df_devices['datetime'][i], df_devices['value'][i]])
    elif df_devices['value'][i] < parameter and alarma:
        alarma = False
        coordenadas.append([df_devices['datetime'][i], df_devices['value'][i]])
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
        
df_analysis = df_final.copy()

df_analysis['year'] = df_analysis['start'].dt.year
df_analysis['period'] = df_analysis['start'].dt.strftime('%Y-%m')
df_analysis['month'] = df_analysis['start'].dt.month_name()
df_analysis['month'] = df_analysis['month'].replace(['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November','December'], ['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio', 'Julio', 'Agosto','Septiembre', 'Octubre', 'Noviembre', 'Diciembre'])
df_analysis['nro_month'] = df_analysis['start'].dt.strftime('%m-%Y')

df_analysis['group'] = df_analysis['start'].dt.date.ne(df_analysis['start'].dt.date.shift()).cumsum()
df_analysis['grouping'] = df_analysis['start'].dt.date
df_analysis['hour_start'] = df_analysis['start'].dt.strftime('%H:%M')
df_analysis['hour_end'] = df_analysis['end'].dt.strftime('%H:%M')
df_analysis['transcurred'] = df_analysis['duration'].apply(lambda x: f'{int(x // 60)}h {int(x % 60)}m')
df_analysis['turn'] = df_analysis['start'].apply(lambda x: 'NOCHE' if 5 <= x.hour < 8 else ('DIA' if 17 <= x.hour < 20 else 'OTROS'))

df_analysis['DIA'] = df_analysis['turn'].apply(lambda x: 1 if x == 'DIA' else 0)
df_analysis['NOCHE'] = df_analysis['turn'].apply(lambda x: 1 if x == 'NOCHE' else 0)
df_analysis = df_analysis.reset_index(drop=True)

df_voladuras = df_analysis.groupby(['grouping']).agg({'DIA': 'sum', 'NOCHE': 'sum', 'duration': 'sum'}).reset_index()
df_voladuras['DIA'] = df_voladuras['DIA'].apply(lambda x: 1 if x > 0 else 0)
df_voladuras['NOCHE'] = df_voladuras['NOCHE'].apply(lambda x: 1 if x > 0 else 0)
df_voladuras['transcurred'] = df_voladuras['duration'].apply(lambda x: f'{int(x // 60)}h {int(x % 60)}m')

# POR SEMANA
# df_voladuras['grouping'] = pd.to_datetime(df_voladuras['grouping'])
# df_voladuras = df_voladuras.set_index('grouping')
# df_voladuras = df_voladuras.resample('D').asfreq()
# df_voladuras['DIA'] = df_voladuras['DIA'].fillna(0)
# df_voladuras['NOCHE'] = df_voladuras['NOCHE'].fillna(0)
# df_voladuras['duration'] = df_voladuras['duration'].fillna(0)
# df_voladuras['transcurred'] = df_voladuras['transcurred'].fillna('0h 0m')
# df_voladuras = df_voladuras.reset_index()

# POR MES
df_voladuras['grouping'] = pd.to_datetime(df_voladuras['grouping'])
df_voladuras = df_voladuras.set_index('grouping')
df_voladuras = df_voladuras.resample('D').asfreq()
start_date = df_voladuras.index.min().to_period('M').to_timestamp()
end_date = df_voladuras.index.max().to_period('M').to_timestamp() + pd.offsets.MonthEnd(1)
full_date_range = pd.date_range(start=start_date, end=end_date, freq='D')
df_voladuras = df_voladuras.reindex(full_date_range)
df_voladuras['DIA'] = df_voladuras['DIA'].fillna(0)
df_voladuras['NOCHE'] = df_voladuras['NOCHE'].fillna(0)
df_voladuras['duration'] = df_voladuras['duration'].fillna(0)
df_voladuras['transcurred'] = df_voladuras['transcurred'].fillna('0h 0m')
df_voladuras = df_voladuras.reset_index()
df_voladuras['DIA'] = df_voladuras['DIA'].astype(int)
df_voladuras['NOCHE'] = df_voladuras['NOCHE'].astype(int)


# CALCULOS

vol_dia = df_voladuras['DIA'].sum()
vol_noche = df_voladuras['NOCHE'].sum()

total_dia = df_voladuras['DIA'].count()
total_noche = df_voladuras['NOCHE'].count()

res_dia = f'{vol_dia} ejecutadas de {total_dia} programadas'
res_noche = f'{vol_noche} ejecutadas de {total_noche} programadas'

df_turn = df_analysis[['grouping', 'hour_start', 'hour_end', 'transcurred', 'duration', 'turn']]
df_turn['min_start'] = df_turn['hour_start'].str.split(':').apply(lambda x: int(x[0]) * 60 + int(x[1]))
df_turn['min_end'] = df_turn['hour_end'].str.split(':').apply(lambda x: int(x[0]) * 60 + int(x[1]))

df_turn['duration'] = df_turn['min_end'] - df_turn['min_start']
df_turn['transcurred'] = df_turn['duration'].apply(lambda x: f'{int(x // 60)}h {int(x % 60)}m')

df_turn = df_turn.query('turn != "OTROS"')

df_dia = df_turn.query('turn == "DIA"')
df_noche = df_turn.query('turn == "NOCHE"')

hour_prom_start_dia = divmod(df_dia['min_start'].mean(), 60)
hour_prom_start_dia = '{:02d}:{:02d}'.format(int(hour_prom_start_dia[0]), int(hour_prom_start_dia[1]))
hour_prom_start_noche = divmod(df_noche['min_start'].mean(), 60)
hour_prom_start_noche = '{:02d}:{:02d}'.format(int(hour_prom_start_noche[0]), int(hour_prom_start_noche[1]))

hour_prom_end_dia = divmod(df_dia['min_end'].mean(), 60)
hour_prom_end_dia = '{:02d}:{:02d}'.format(int(hour_prom_end_dia[0]), int(hour_prom_end_dia[1]))
hour_prom_end_noche = divmod(df_noche['min_end'].mean(), 60)
hour_prom_end_noche = '{:02d}:{:02d}'.format(int(hour_prom_end_noche[0]), int(hour_prom_end_noche[1]))

hour_dia = df_dia['duration'].mean()
mean_dia = f'{int(hour_dia // 60)}h {int(hour_dia % 60)}m'
hour_noche = df_noche['duration'].mean()
mean_noche = f'{int(hour_noche // 60)}h {int(hour_noche % 60)}m'