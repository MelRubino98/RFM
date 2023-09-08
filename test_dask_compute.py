import pandas as pd
import dask.dataframe as dd
import os
import pytest
import vaex
import gc
import psutil
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import sys
import warnings

if not sys.warnoptions:
    warnings.simplefilter("ignore")

class Test:
    
    def test_directorio(self):
        print('Directorio: ', os.listdir())
        assert True    





    '''def test_groupby_dask_compute_1M(self):
        print('groupby_dask_compute_1M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        

        df = dd.read_csv('work/2019-Oct-1M.csv')
        df.groupby('user_id').agg({'price':['sum']}).reset_index().compute()  

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')          
       
        assert True   


    def test_groupby_dask_compute_5M(self):
        print('groupby_dask_compute_5M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        

        df = dd.read_csv('work/2019-Oct-5M.csv')
        df.groupby('user_id').agg({'price':['sum']}).reset_index().compute()  

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')          
        assert True   

    def test_groupby_dask_compute_10M(self):
        print('groupby_dask_compute_10M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        

        df = dd.read_csv('work/2019-Oct-10M.csv')
        df.groupby('user_id').agg({'price':['sum']}).reset_index().compute()  

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')          
        assert True   

    def test_groupby_dask_compute_25M(self):
        print('groupby_dask_compute_25M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        

        df = dd.read_csv('work/2019-Oct-25M.csv')
        df.groupby('user_id').agg({'price':['sum']}).reset_index().compute()  

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')          
        assert True 

    def test_groupby_dask_compute_total(self):
        print('groupby_dask_compute_total')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        

        df = dd.read_csv('work/2019-Oct-Nov.csv')
        df.groupby('user_id').agg({'price':['sum']}).reset_index().compute()  

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')          
        assert True 


    def test_del_column_dask_compute_1M(self):
        print('del_column_dask_compute_1M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        
        df = dd.read_csv('work/2019-Oct-1M.csv')
        df = df.drop(columns=['user_session']).compute()

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')          

        assert True   

    def test_del_column_dask_compute_5M(self):
        print('del_column_dask_compute_5M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        
        df = dd.read_csv('work/2019-Oct-5M.csv')
        df = df.drop(columns=['user_session']).compute()

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')          
        assert True   

    def test_del_column_dask_compute_10M(self):
        print('del_column_dask_compute_10M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        
        df = dd.read_csv('work/2019-Oct-10M.csv')
        df = df.drop(columns=['user_session']).compute()

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')          
        assert True   

    def test_del_column_dask_compute_25M(self):
        print('del_column_dask_compute_25M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        
        df = dd.read_csv('work/2019-Oct-25M.csv')
        df = df.drop(columns=['user_session']).compute()

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')          
        assert True 

    def test_del_column_dask_compute_total(self):
        print('del_column_dask_compute_total')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        
        df = dd.read_csv('work/2019-Oct-Nov.csv')
        df = df.drop(columns=['user_session']).compute()

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')          
        assert True 




    def test_filter_dask_compute_1M(self):
        print('filter_dask_compute_1M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        

        df = dd.read_csv('work/2019-Oct-1M.csv')
        df = df.loc[(df["event_type"] == 'purchase')].compute()
        
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       

        assert True   

    def test_filter_dask_compute_5M(self):
        print('filter_dask_compute_5M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        

        df = dd.read_csv('work/2019-Oct-5M.csv')
        df = df.loc[(df["event_type"] == 'purchase')].compute()
        
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       
        assert True   

    def test_filter_dask_compute_10M(self):
        print('filter_dask_compute_10M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        

        df = dd.read_csv('work/2019-Oct-10M.csv')
        df = df.loc[(df["event_type"] == 'purchase')].compute()
        
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       
        assert True   

    def test_filter_dask_compute_25M(self):
        print('filter_dask_compute_25M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        

        df = dd.read_csv('work/2019-Oct-25M.csv')
        df = df.loc[(df["event_type"] == 'purchase')].compute()
        
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       
        assert True 

    def test_filter_dask_compute_total(self):
        print('filter_dask_compute_total')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        

        df = dd.read_csv('work/2019-Oct-Nov.csv')
        df = df.loc[(df["event_type"] == 'purchase')].compute()
        
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       
        assert True 


    def test_drop_duplicates_dask_compute_1M(self):
            print('test_drop_duplicates_dask_1M')
            gc.collect()
            print('\nRAM ocupada ANTES del test: ')
            print(psutil.virtual_memory().percent)
            print('%')        
            df = dd.read_csv('work/2019-Oct-1M.csv')
            df.drop_duplicates().compute()
            print('\nRAM ocupada DESPUÉS del test: ')
            print(psutil.virtual_memory().percent)        
            print('%\n')       

            assert True

    def test_drop_duplicates_dask_5M(self):
        print('test_drop_duplicates_dask_5M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        df = dd.read_csv('work/2019-Oct-5M.csv')
        df.drop_duplicates().compute()
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       

        assert True

    def test_drop_duplicates_dask_10M(self):
        print('test_drop_duplicates_dask_10M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        df = dd.read_csv('work/2019-Oct-10M.csv')
        df.drop_duplicates().compute()
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       

        assert True

    def test_drop_duplicates_dask_25M(self):
        print('test_drop_duplicates_dask_25M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        df = dd.read_csv('work/2019-Oct-25M.csv')
        df.drop_duplicates().compute()
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       

        assert True

   
    def test_drop_duplicates_dask_total(self):
        print('test_drop_duplicates_dask_total')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        df = dd.read_csv('work/2019-Oct-Nov.csv')
        df.drop_duplicates().compute()
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       

        assert True'''
