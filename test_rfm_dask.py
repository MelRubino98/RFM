# -*- coding: utf-8 -*-
"""
Created on Wed Feb  1 14:49:27 2023

@author: melanie.g.rubino
"""

import os
import pytest
import pandas as pd
import dask.dataframe as dd
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

    def test_rfm_dask(self):
        print('test_rfm_dask')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')         

        df = dd.read_csv('work/RFM/2019-Oct-Nov-transformed.csv')
        
        #RECENCY
        window= np.datetime64('2019-12-01','ns')
        window = window.astype("datetime64[D]")
        df_recency = df.groupby('user_id').agg({'event_time': ['max']}).reset_index()
        df_recency.columns = df_recency.columns.droplevel(1)
        df_recency['event_time']= dd.to_datetime(df_recency['event_time'],format='%Y-%m-%d')
        df_recency['Recency'] = df_recency['event_time'].apply(lambda x: (window - x)).dt.days
        df_recency = df_recency[['user_id','Recency']]
        df_recency.compute()
        
        #FREQUENCY
        df_frequency = df.drop_duplicates().groupby('user_id').agg({'event_time': ['count']}).reset_index()
        df_frequency.columns = df_frequency.columns.droplevel(1)
        df_frequency= df_frequency.rename(columns={'event_time':'Frequency'})
        df_frequency.compute()
        
        #MONETARY
        df_monetary = df.groupby('user_id').agg({'price':['sum']}).reset_index()
        df_monetary.columns = df_monetary.columns.droplevel(1)
        df_monetary=df_monetary.rename(columns={'price':'Monetary'})
        df_monetary.compute()        

        #MERGE DE LOS TRES DF
        df_rfm = df_recency.merge(df_frequency, how='inner')
        df_rfm = df_rfm.merge(df_monetary, how = 'inner')
        
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
        p98 = df_rfm['Frequency'].quantile(0.98)
        print('Percentil 98: ' + str(p98))
        df_rfm = df_rfm[df_rfm.Frequency <= p98]
        #Para Monetary, calculamos el percentil 2 y el percentil 98 (p2 y p98) y nos quedamos con el intervalo entre estos dos valores.
        p2 = df_rfm['Monetary'].quantile(0.02)
        p98 = df_rfm['Monetary'].quantile(0.98)
        print('Percentil 2: ' + str(p2))
        print('Percentil 98: ' + str(p98))
        df_rfm = df_rfm[df_rfm.Monetary > p2]
        df_rfm = df_rfm[df_rfm.Monetary < p98]
        
        #ASIGNAMOS QUINTILES
        rfm = df_rfm.compute()
        R_condition = [(rfm['Recency'] < rfm['Recency'].quantile(0.2)),
                    ((rfm['Recency'] >= rfm['Recency'].quantile(0.2)) & (rfm['Recency'] < rfm['Recency'].quantile(0.4))),
                    ((rfm['Recency'] >= rfm['Recency'].quantile(0.4)) & (rfm['Recency'] < rfm['Recency'].quantile(0.6))),
                    ((rfm['Recency'] >= rfm['Recency'].quantile(0.6)) & (rfm['Recency'] < rfm['Recency'].quantile(0.8))),
                    (rfm['Recency'] >= rfm['Recency'].quantile(0.8))]
        option = [5,4,3,2,1]
        rfm['R'] = np.select(R_condition, option,default=5)
        F_condition = [(rfm['Frequency'] == 1) | (rfm['Frequency'] == 2),
            (rfm['Frequency'] == 3) | (rfm['Frequency'] == 4),
            (rfm['Frequency'] == 5) | (rfm['Frequency'] == 6),
            (rfm['Frequency'] == 7) | (rfm['Frequency'] == 8),
            (rfm['Frequency'] >= 9)]
        option = [1,2,3,4,5]
        rfm['F'] = np.select(F_condition, option,default=5)
        M_condition = [(rfm['Monetary'] < rfm['Monetary'].quantile(0.2)),
            ((rfm['Monetary'] >= rfm['Monetary'].quantile(0.2)) & (rfm['Monetary'] < rfm['Monetary'].quantile(0.4))),
            ((rfm['Monetary'] >= rfm['Monetary'].quantile(0.4)) & (rfm['Monetary'] < rfm['Monetary'].quantile(0.6))),
            ((rfm['Monetary'] >= rfm['Monetary'].quantile(0.6)) & (rfm['Monetary'] < rfm['Monetary'].quantile(0.8))),
            (rfm['Monetary'] >= rfm['Monetary'].quantile(0.8))]
        option = [1,2,3,4,5]
        rfm['M'] = np.select(M_condition, option,default=5)

        rfm["RFM_SCORE"] = (rfm['R'].astype(str) + 
                            rfm['F'].astype(str) +
                            rfm['M'].astype(str))
        
        #A PARTIR DEL RFM_SCORE, MAPEAMOS LOS SEGMENTOS
        seg_map = dd.read_csv('work/RFM/segmentos_rfm.csv', usecols=['RFM_SCORE','segment']).compute()
        seg_map['RFM_SCORE']= seg_map.RFM_SCORE.astype(str)
        rfm = rfm.merge(seg_map, on='RFM_SCORE')
        
        #EXPORTAMOS EL DATASET
        rfm.to_csv('work/RFM/RFM DASK Resultados.csv')

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       

    assert True

