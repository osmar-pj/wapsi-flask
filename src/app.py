from flask import Flask, jsonify, request, Response
from datetime import datetime
from pymongo import MongoClient
from datetime import datetime, timedelta
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
datas = db['datas']

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
        dfRes['datetime'] = pd.to_datetime(dfRes['ts'], unit='s')
        _df = dfRes[['datetime','value']].copy()
        df = _df.resample('1min',on='datetime').mean().ffill().bfill()
        df['ts'] = df.index.astype(np.int64) // 10**9
        df.reset_index(drop=True, inplace=True)
        data = df.to_dict(orient='records')
        return jsonify({'data': data})
    except Exception as e:
        return jsonify({'message': str(e)})

@app.route('/advancedAnalysis', methods=['POST'])
def advancedAnalysis():
    try:
        result = request.get_json()
        df = pd.DataFrame(result)
        df['datetime'] = pd.to_datetime(df['ts'], unit='s')
        dbars = df[["datetime", "color"]].copy()
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
        for i in range(len(df)):
            if df['value'][i] >= parameter and not alarma:
                alarma = True
                coordenadas.append([df['datetime'][i], df['value'][i]])
            elif df['value'][i] < parameter and alarma:
                alarma = False
                coordenadas.append([df['datetime'][i], df['value'][i]])
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
    except Exception as e:
        return jsonify({'message': str(e)})

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