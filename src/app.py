from flask import Flask, jsonify, request, Response
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

from models.analysis.list import getWeekAnalysis
from models.analysis.list import getMonthAnalysis

app = Flask(__name__)

client = MongoClient(os.getenv('MONGO_URI'))
db = client['wapsi']
# datas = db['datas']
datas = db['bigdatas']

# NEW VERSION MONTH-WEEK DASHBOARD

# @app.route('/analysis', methods=['GET'])
# def get_analysis():
    
#     serie = request.args.get('serie')
#     name = request.args.get('name')
#     start = request.args.get('start')
#     end = request.args.get('end')
#     nro_month = request.args.get('nro_month')
    
#     week = getWeekAnalysis(serie, name, start, end)
#     month = getMonthAnalysis(serie, name, nro_month)
    
#     # return jsonify({'week': week})
#     return jsonify({'week': week, 'month': month})

# OLD VERSION - UPDATED
@app.route('/analysis', methods=['GET'])
def get_analysis():
    datas = db['datas']
    serie = request.args.get('serie')
    name = request.args.get('name')
    start = request.args.get('start')
    end = request.args.get('end')
    
    start_seconds = int(start) / 1000
    end_seconds = int(end) / 1000
    date_start = datetime.fromtimestamp(start_seconds)
    date_end = datetime.fromtimestamp(end_seconds)
    start_tz = pytz.timezone('America/Lima').localize(date_start)
    end_tz = pytz.timezone('America/Lima').localize(date_end)
    df_datas = pd.DataFrame(list(datas.find({'serie': serie, 'name': name, 'createdAt': {'$gte': start_tz, '$lte': end_tz}})))

    if len(df_datas) == 0:
        print('No hay datos')
        return jsonify({'mean': 0, 'data_final': [], 'data': [], 'bars': []})
    
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
        
    dfinal_dict = df_final.to_dict(orient='records')
    dbars_dict = dbars.to_dict(orient='records')
    return jsonify({'mean': mean, 'data_final': dfinal_dict, 'bars': dbars_dict})

@app.route('/basicAnalysis', methods=['POST'])
def basicAnalysis():
    try:
        result = request.get_json()
        dfRes = pd.DataFrame(result)
        if len(dfRes) == 0:
            return jsonify({'data': []})
        dfRes['datetime'] = pd.to_datetime(dfRes['ts'], unit='s')
        _df = dfRes[['datetime','value']].copy()
        df = _df.resample('1min',on='datetime').mean().ffill().bfill()
        df['ts'] = df.index.astype(np.int64) // 10**9
        df.reset_index(drop=True, inplace=True)
        data = df.to_dict(orient='records')
        return jsonify({'data': data})
    except Exception as e:
        return jsonify({'message': str(e)})

#region OLD VERSION
# @app.route('/advancedAnalysis', methods=['POST'])
# def advancedAnalysis():
#     try:
#         result = request.get_json()
#         df = pd.DataFrame(result)
#         df['datetime'] = pd.to_datetime(df['ts'], unit='s')
#         dbars = df[["datetime", "color"]].copy()
#         dbars['x'] = dbars['datetime'].dt.strftime("%H:%M")
#         dbars['y'] = dbars['datetime'].dt.strftime("%Y-%m-%d")
#         dbars.drop('datetime', axis = 1)
#         dbars = dbars[["x", "y", "color"]]
#         dbars['color_diff'] = dbars['color'].ne(dbars['color'].shift())
#         dbars['color_diff'] = dbars['color_diff'].cumsum()
#         dbars = dbars.groupby(['y', 'color_diff', 'color']).last().reset_index()

#         coordenadas = []
#         alarma = False
#         parameter = 25
#         for i in range(len(df)):
#             if df['value'][i] >= parameter and not alarma:
#                 alarma = True
#                 coordenadas.append([df['datetime'][i], df['value'][i]])
#             elif df['value'][i] < parameter and alarma:
#                 alarma = False
#                 coordenadas.append([df['datetime'][i], df['value'][i]])
#         diferencia = []
#         start_df = []
#         end_df = []
#         mean = 0
#         for i in range(0, len(coordenadas), 2):
#             if i + 1 < len(coordenadas):
#                 s = coordenadas[i][0]
#                 e = coordenadas[i + 1][0]
#                 time_dif = e - s
#                 diferencia.append((time_dif))
#                 start_df.append(s)
#                 end_df.append(e)
#         df_final = pd.DataFrame({'start': start_df, 'end': end_df, 'duration': diferencia})

#         if len(df_final) > 0:
#             df_final['duration'] = df_final['duration'].dt.total_seconds() / 60
#             df_final['tstart'] = df_final['start'].astype('int64') / 1e6
#             df_final['tend'] = df_final['end'].astype('int64') / 1e6
#             df_final['day'] = df_final['start'].dt.day_name()
#             df_final['day'] = df_final['day'].replace(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'], ['Lunes', 'Martes', 'Miercoles', 'Jueves', 'Viernes', 'Sabado', 'Domingo'])
#             df_final = df_final.query('duration > 20')
#             if len(df_final) > 0:
#                 df_final['timestart'] = pd.to_datetime(df_final['start'])
#                 df_final = df_final.reset_index(drop=True)
#                 mean = df_final['duration'].mean()
            
#         dfinal_dict = df_final.to_dict(orient='records')
#         dbars_dict = dbars.to_dict(orient='records')
#         return jsonify({'mean': mean, 'data_final': dfinal_dict, 'bars': dbars_dict})
#     except Exception as e:
#         return jsonify({'message': str(e)})

#region NEW VERSION
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

def complete_empty(start, end):
    start_date = datetime.fromtimestamp(start)
    end_date = datetime.fromtimestamp(end)
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    missing_days = pd.DataFrame({'datetime': date_range})
    missing_days['y'] = missing_days['datetime'].dt.strftime('%Y-%m-%d')
    missing_days['x'] = '23:59'
    missing_days['color'] = '#898989'
    return missing_days[['y', 'color', 'x']]

@app.route('/advancedAnalysis', methods=['POST'])
def advancedAnalysis():
    
    result = request.get_json()
    start = int(result['startTs'])
    end = int(result['endTs'])
    data = result['result']
    
    if len(data) == 0:
        dbars = complete_empty(start, end)
        dbars_dict = dbars.to_dict(orient='records')    
        return jsonify({'mean': 0, 'data_final': [], 'bars': dbars_dict})
    
    df_data = pd.DataFrame(data)
    
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
    
    dbars_completed = complete_days(dbars, start, end)
    
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
    
    dfinal_dict = df_final.to_dict(orient='records')
    dbars_dict = dbars_completed.to_dict(orient='records')
    return jsonify({'mean': mean, 'data_final': dfinal_dict, 'bars': dbars_dict})

#region FINAL
@app.errorhandler(404)
def not_found(error=None):
    response = jsonify({
        'message': 'Resource Not Found: ' + request.url,
        'status': 404
    })
    response.status_code = 404
    return response

if __name__ == '__main__':
    # app.run(debug=True)
    app.run(debug=True, host='0.0.0.0', port=8082)