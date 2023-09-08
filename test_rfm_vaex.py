import os
import pytest
import pandas as pd
import vaex
from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt
import sys
import warnings
import gc
import psutil

class Test:
    def test_directorio(self):
        print('Directorio: ', os.listdir())
        assert True
    
    def test_rfm_vaex(self):
        print('test_rfm_vaex')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')         

        df=vaex.from_csv('work/RFM/2019-Oct-Nov-transformed.csv', parse_dates=['event_time'])
        df['event_time'] = df[('event_time')].astype("datetime64[D]")

        #RECENCY
        window= np.datetime64('2019-12-01')
        window = window.astype("datetime64[D]")
        df_recency = df.groupby(by=['user_id'], agg=({'event_time': 'max'}))
        df_recency['event_time'] = df_recency['event_time'].values.astype("datetime64[D]")
        df_recency['Recency'] =  (df_recency['event_time'] - window) * (-1)
        df_recency = df_recency[['user_id','Recency']]
        df_recency['Recency'] = df_recency[('Recency')].values.astype("int")

        #FREQUENCY
        df_frequency = df.groupby(by=['user_id'], agg='count')
        df_frequency.rename('count','Frequency')

        #MONETARY
        df_monetary = df.groupby(by=['user_id'], agg=({'price': 'sum'}))
        df_monetary.rename('price','Monetary')

        #MERGE DE LOS TRES DF
        df_rfm = df_recency.join(df_frequency, 
                                how='inner', 
                                left_on ='user_id',
                                right_on='user_id')

        df_rfm = df_rfm.join(df_monetary, 
                                how='inner', 
                                left_on ='user_id',
                                right_on='user_id')

        #ELIMINAMOS OUTLIERS UTILIZANDO EL PERCENTIL 98
        #Chequeamos máximos y mínimos en Recency (no puede ser menor a 1 ni mayor a 61)
        print(df_rfm['Recency'].min())
        print(df_rfm['Recency'].max())
        #Chequeamos máximos y mínimos en Frequency
        print(df_rfm['Frequency'].min())
        print(df_rfm['Frequency'].max())
        #Chequeamos máximos y mínimos en Monetary
        print(df_rfm['Monetary'].min())
        print(df_rfm['Monetary'].max())

        #Para Frequency, calculamos el percentil 98 (q98) y nos quedamos con los valores menores a él.
        p98 = df_rfm.percentile_approx('Frequency', 98)
        print('Percentil 98: ' + str(p98))
        df_rfm = df_rfm[df_rfm.Frequency <= p98]

        #Para Monetary, calculamos el percentil 2 y el percentil 98 (p2 y p98) y nos quedamos con el intervalo entre estos dos valores.
        p2 = df_rfm.percentile_approx('Monetary', 2)
        p98 = df_rfm.percentile_approx('Monetary', 98)
        print('Percentil 2: ' + str(p2))
        print('Percentil 98: ' + str(p98))
        df_rfm = df_rfm[df_rfm.Monetary > p2]
        df_rfm = df_rfm[df_rfm.Monetary < p98]

        #ASIGNAMOS QUINTILES
        rfm = pd.DataFrame(df_rfm, columns=['user_id','Recency','Frequency','Monetary'])
        rfm = rfm.astype({'user_id':'int','Recency':'int','Frequency':'int'})

        R_condition = [(rfm['Recency'] < 11),
                    ((rfm['Recency'] >= 11) & (rfm['Recency'] < 15)),
                    ((rfm['Recency'] >= 15) & (rfm['Recency'] < 28)),
                    ((rfm['Recency'] >= 28) & (rfm['Recency'] < 44)),
                    (rfm['Recency'] >= 44)]

        option = [5,4,3,2,1]
        rfm['R'] = np.select(R_condition, option,default=5)
        condition = [(rfm.Frequency == 1) | (rfm.Frequency == 2),
                    (rfm.Frequency == 3) | (rfm.Frequency == 4),
                    (rfm.Frequency == 5) | (rfm.Frequency == 6),
                    (rfm.Frequency == 7) | (rfm.Frequency == 8)]
        option = [1,2,3,4]
        rfm['F'] = np.select(condition, option, default= 5)

        rfm['M'] = pd.qcut(rfm['Monetary'], q=5, labels=[1,2,3,4,5]) 
        rfm['M']=rfm['M'].astype(int)
        rfm = vaex.from_pandas(rfm)
        rfm["RFM_SCORE"] = (rfm['R'].astype(str) + 
                            rfm['F'].astype(str) +
                            rfm['M'].astype(str))

        #A PARTIR DEL RFM_SCORE, MAPEAMOS LOS SEGMENTOS
        seg_map = vaex.read_csv('work/RFM/segmentos_rfm.csv', usecols=['RFM_SCORE','segment'])
        seg_map['RFM_SCORE']= seg_map.RFM_SCORE.astype(str)

        rfm = rfm.join(seg_map, 
                                how='inner', 
                                left_on ='RFM_SCORE',
                                right_on='RFM_SCORE')


        rfm.export_csv('work/RFM/RFM VAEX Resultados.csv')

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       

    
    
    assert True
