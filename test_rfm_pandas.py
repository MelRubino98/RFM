# -*- coding: utf-8 -*-
"""
Created on Wed Feb  1 14:28:23 2023

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
    
    def test_rfm_pandas(self):
        print('test_rfm_pandas')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')         


        df=pd.read_csv('work/RFM/2019-Oct-Nov-transformed.csv')
        df['event_time'] =  pd.to_datetime(df['event_time'], format='%Y-%m-%d')
        
        
        #RECENCY
        window= np.datetime64('2019-12-01','ns')
        window = window.astype("datetime64[D]")
        df_recency = df.groupby(by='user_id', as_index=False)['event_time'].max()
        df_recency['Recency'] = df_recency['event_time'].apply(lambda x: (window - x).days)
        df_recency = df_recency[['user_id','Recency']]
        
        #FREQUENCY
        df_frequency = df.drop_duplicates().groupby(by=['user_id'], as_index=False)['event_time'].count()
        df_frequency.rename(columns={'event_time':'Frequency'}, inplace=True)
        
        #MONETARY
        df_monetary = df.groupby(by='user_id', as_index=False)['price'].sum()
        df_monetary.rename(columns={'price':'Monetary'}, inplace=True)
        
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
        
        df_rfm.reset_index(inplace=True, drop=True)
        df_rfm.info()
        
        #ASIGNAMOS QUINTILES
        rfm = df_rfm
        rfm['R'], bins = pd.qcut(rfm['Recency'], q=5, labels=[5,4,3,2,1], retbins=True)
        condition = [(rfm.Frequency == 1) | (rfm.Frequency == 2),
                    (rfm.Frequency == 3) | (rfm.Frequency == 4),
                    (rfm.Frequency == 5) | (rfm.Frequency == 6),
                    (rfm.Frequency == 7) | (rfm.Frequency == 8)]
        option = [1,2,3,4]
        rfm['F'] = np.select(condition, option, default= 5)
        
        rfm['M'] = pd.qcut(rfm['Monetary'], q=5, labels=[1,2,3,4,5])
        
        rfm["RFM_SCORE"] = (rfm['R'].astype(str) + 
                            rfm['F'].astype(str) +
                            rfm['M'].astype(str))
        
        #A PARTIR DEL RFM_SCORE, MAPEAMOS LOS SEGMENTOS.
        seg_map = pd.read_csv('work/RFM/segmentos_rfm.csv', usecols=['RFM_SCORE','segment'])
        seg_map['RFM_SCORE']= seg_map.RFM_SCORE.astype(str)
        rfm = rfm.merge(seg_map, on='RFM_SCORE')
        
        #EXPORTAMOS EL DATASET
        rfm.to_csv('work/RFM/RFM PANDAS Resultados.csv')
        
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       

    
    
    assert True
