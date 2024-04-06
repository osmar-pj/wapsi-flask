import pandas as pd
import numpy as np

data = pd.read_csv('data.csv')

data['datetime'] = pd.to_datetime(data['ts'], unit='s')
_df = data[['datetime','value']].copy()
df = _df.resample('1min',on='datetime').mean().ffill().bfill()
df['ts'] = df.index.astype(np.int64) // 10**9
df.reset_index(drop=True, inplace=True)
