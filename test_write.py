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

    def test_write_pandas_1M_parquet(self):
        print('test_write_pandas_1M_parquet')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')
    
        df = pd.read_csv('work/2019-Oct-1M.csv')
        df.to_parquet('work/Write outputs/2019-Oct-1M.parquet')
        
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)
        print('%\n')
       
        assert True

    def test_write_pandas_5M_parquet(self):
        print('test_write_pandas_5M_parquet')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')
    
        df = pd.read_csv('work/2019-Oct-5M.csv')
        df.to_parquet('work/Write outputs/2019-Oct-5M.parquet')

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)
        print('%\n')
        
        assert True
        
    def test_write_pandas_10M_parquet(self):
        print('test_write_pandas_10M_parquet')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')
        
        df = pd.read_csv('work/2019-Oct-10M.csv')
        df.to_parquet('work/Write outputs/2019-Oct-10M.parquet')

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)
        print('%\n')

        assert True
    
    def test_write_pandas_25M_parquet(self):
        print('test_write_pandas_25M_parquet')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')
    
        df = pd.read_csv('work/2019-Oct-25M.csv')
        df.to_parquet('work/Write outputs/2019-Oct-25M.parquet')

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)
        print('%\n')

        assert True

    def test_read_pandas_total(self):
        print('test_read_pandas_total')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')

        df = pd.read_csv('work/2019-Oct-Nov.csv')

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)
        print('%\n')
        assert True


    def test_write_pyspark_1M(self):
        print('test_write_pyspark_1M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')


        #initialise sparkContext
        spark = SparkSession.builder \
                        .master('local') \
                        .appName('myAppName') \
                        .config('spark.executor.memory', '5gb') \
                        .config("spark.cores.max", "6") \
                        .getOrCreate()

        sc = spark.sparkContext
        # using SQLContext to read file
        sqlContext = SQLContext(sc)
        df = sqlContext.read.csv('work/2019-Oct-1M.csv', header='True')
        df.repartition(1).write.csv('work/Write outputs/2019-Oct-1M.csv')
        #df.write.format('csv').save('work/Write outputs/2019-Oct-1M.csv')
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)
        print('%\n')
       
        assert True

    def test_write_pyspark_5M(self):
        print('test_write_pyspark_5M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')
    
        #initialise sparkContext
        spark = SparkSession.builder \
                        .master('local') \
                        .appName('myAppName') \
                        .config('spark.executor.memory', '5gb') \
                        .config("spark.cores.max", "6") \
                        .getOrCreate()

        sc = spark.sparkContext
        # using SQLContext to read file
        sqlContext = SQLContext(sc)
        df = sqlContext.read.csv('work/2019-Oct-5M.csv', header='True')
        df.repartition(1).write.csv('work/Write outputs/2019-Oct-5M.csv')

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)
        print('%\n')
        
        assert True
        
    def test_write_pyspark_10M(self):
        print('test_write_pyspark_10M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')
        
        #initialise sparkContext
        spark = SparkSession.builder \
                        .master('local') \
                        .appName('myAppName') \
                        .config('spark.executor.memory', '5gb') \
                        .config("spark.cores.max", "6") \
                        .getOrCreate()

        sc = spark.sparkContext
        # using SQLContext to read file
        sqlContext = SQLContext(sc)
        df = sqlContext.read.csv('work/2019-Oct-10M.csv', header='True')
        df.repartition(1).write.csv('work/Write outputs/2019-Oct-10M.csv')

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)
        print('%\n')

        assert True
    
    def test_write_pyspark_25M(self):
        print('test_write_pyspark_25M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')
    
        #initialise sparkContext
        spark = SparkSession.builder \
                        .master('local') \
                        .appName('myAppName') \
                        .config('spark.executor.memory', '5gb') \
                        .config("spark.cores.max", "6") \
                        .getOrCreate()

        sc = spark.sparkContext
        # using SQLContext to read file
        sqlContext = SQLContext(sc)
        df = sqlContext.read.csv('work/2019-Oct-25M.csv', header='True')
        df.repartition(1).write.csv('work/Write outputs/2019-Oct-25M.csv')

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)
        print('%\n')

        assert True

    def test_write_pyspark_total(self):
        print('test_read_pyspark_total')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')

        #initialise sparkContext
        spark = SparkSession.builder \
                        .master('local') \
                        .appName('myAppName') \
                        .config('spark.executor.memory', '5gb') \
                        .config("spark.cores.max", "6") \
                        .getOrCreate()

        sc = spark.sparkContext
        # using SQLContext to read file
        sqlContext = SQLContext(sc)
        df = sqlContext.read.csv('work/2019-Oct-Nov.csv', header='True')
        df.repartition(1).write.csv('work/Write outputs/2019-Oct-Nov.csv')

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)
        print('%\n')
        assert True


    def test_write_pandas_1M(self):
        print('test_write_pandas_1M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')
    
        df = pd.read_csv('work/2019-Oct-1M.csv')
        df.to_csv('work/Write outputs/2019-Oct-1M.csv')
        
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)
        print('%\n')
       
        assert True

    def test_write_pandas_5M(self):
        print('test_write_pandas_5M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')
    
        df = pd.read_csv('work/2019-Oct-5M.csv')
        df.to_csv('work/Write outputs/2019-Oct-5M.csv')

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)
        print('%\n')
        
        assert True
        
    def test_write_pandas_10M(self):
        print('test_write_pandas_10M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')
        
        df = pd.read_csv('work/2019-Oct-10M.csv')
        df.to_csv('work/Write outputs/2019-Oct-10M.csv')

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)
        print('%\n')

        assert True
    
    def test_write_pandas_25M(self):
        print('test_write_pandas_25M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')
    
        df = pd.read_csv('work/2019-Oct-25M.csv')
        df.to_csv('work/Write outputs/2019-Oct-25M.csv')

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)
        print('%\n')

        assert True

    def test_read_pandas_total(self):
        print('test_read_pandas_total')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')

        df = pd.read_csv('work/2019-Oct-Nov.csv')

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)
        print('%\n')
        assert True


    def test_write_dask_1M(self):
        print('test_write_dask_1M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')

        df = dd.read_csv('work/2019-Oct-1M.csv').compute()
        df.to_csv('work/Write outputs/2019-Oct-1M.csv')

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)
        print('%\n')

        assert True
    
    def test_write_dask_5M(self):
        print('test_write_dask_5M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        df = dd.read_csv('work/2019-Oct-5M.csv').compute()
        df.to_csv('work/Write outputs/2019-Oct-5M.csv')

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)
        print('%\n')

        assert True

    def test_write_dask_10M(self):
        print('test_write_dask_10M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        df = dd.read_csv('work/2019-Oct-10M.csv').compute()
        df.to_csv('work/Write outputs/2019-Oct-10M.csv')

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)
        print('%\n')       
        assert True

    def test_write_dask_25M(self):
        print('test_write_dask_25M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        df = dd.read_csv('work/2019-Oct-25M.csv').compute()
        df.to_csv('work/Write outputs/2019-Oct-25M.csv')

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)
        print('%\n')       
        assert True

    def test_write_dask_total(self):
        print('test_write_dask_total')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        df = dd.read_csv('work/2019-Oct-Nov.csv').compute()
        df.to_csv('work/Write outputs/2019-Oct-Nov.csv')
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)
        print('%\n')       
        assert True  

    def test_write_vaex_1M(self):
        print('test_write_vaex_1M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        df = vaex.open('work/2019-Oct-1M.csv')
        df.export_csv('work/Write outputs/2019-Oct-1M.csv')
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       
        assert True

    def test_write_vaex_5M(self):
        print('test_write_vaex_5M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        df = vaex.open('work/2019-Oct-5M.csv')
        df.export_csv('work/Write outputs/2019-Oct-5M.csv')
   
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       

        assert True

    def test_write_vaex_10M(self):
        print('test_write_vaex_10M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        df = vaex.open('work/2019-Oct-10M.csv')
        df.export_csv('work/Write outputs/2019-Oct-10M.csv')

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       

        assert True   

    def test_write_vaex_25M(self):
        print('test_write_vaex_25M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        df = vaex.open('work/2019-Oct-25M.csv')
        df.export_csv('work/Write outputs/2019-Oct-25M.csv')

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       

        assert True        
    
    def test_write_vaex_total(self):
        print('test_write_vaex_total')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        df = vaex.open('work/2019-Oct-Nov.csv')
        df.export_csv('work/Write outputs/2019-Oct-Nov.csv')
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       

        assert True  


    def test_write_pyspark_1M_parquet(self):
        print('test_write_pyspark_1M_parquet')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')


        #initialise sparkContext
        spark = SparkSession.builder \
                        .master('local') \
                        .appName('myAppName') \
                        .config('spark.executor.memory', '5gb') \
                        .config("spark.cores.max", "6") \
                        .getOrCreate()

        sc = spark.sparkContext
        # using SQLContext to read file
        sqlContext = SQLContext(sc)
        df = sqlContext.read.csv('work/2019-Oct-1M.csv', header='True')
        df.repartition(1).write.parquet('work/Write outputs/2019-Oct-1M.parquet')

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)
        print('%\n')
       
        assert True

    def test_write_pyspark_5M_parquet(self):
        print('test_write_pyspark_5M_parquet')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')
    
        #initialise sparkContext
        spark = SparkSession.builder \
                        .master('local') \
                        .appName('myAppName') \
                        .config('spark.executor.memory', '5gb') \
                        .config("spark.cores.max", "6") \
                        .getOrCreate()

        sc = spark.sparkContext
        # using SQLContext to read file
        sqlContext = SQLContext(sc)
        df = sqlContext.read.csv('work/2019-Oct-5M.csv', header='True')
        df.repartition(1).write.parquet('work/Write outputs/2019-Oct-5M.parquet')

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)
        print('%\n')
        
        assert True
        
    def test_write_pyspark_10M_parquet(self):
        print('test_write_pyspark_10M_parquet')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')
        
        #initialise sparkContext
        spark = SparkSession.builder \
                        .master('local') \
                        .appName('myAppName') \
                        .config('spark.executor.memory', '5gb') \
                        .config("spark.cores.max", "6") \
                        .getOrCreate()

        sc = spark.sparkContext
        # using SQLContext to read file
        sqlContext = SQLContext(sc)
        df = sqlContext.read.csv('work/2019-Oct-10M.csv', header='True')
        df.repartition(1).write.parquet('work/Write outputs/2019-Oct-10M.parquet')

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)
        print('%\n')

        assert True
    
    def test_write_pyspark_25M_parquet(self):
        print('test_write_pyspark_25M_parquet')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')
    
        #initialise sparkContext
        spark = SparkSession.builder \
                        .master('local') \
                        .appName('myAppName') \
                        .config('spark.executor.memory', '5gb') \
                        .config("spark.cores.max", "6") \
                        .getOrCreate()

        sc = spark.sparkContext
        # using SQLContext to read file
        sqlContext = SQLContext(sc)
        df = sqlContext.read.csv('work/2019-Oct-25M.csv', header='True')
        df.repartition(1).write.parquet('work/Write outputs/2019-Oct-25M.parquet')

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)
        print('%\n')

        assert True

    def test_write_pyspark_total_parquet(self):
        print('test_read_pyspark_total_parquet')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')

        #initialise sparkContext
        spark = SparkSession.builder \
                        .master('local') \
                        .appName('myAppName') \
                        .config('spark.executor.memory', '5gb') \
                        .config("spark.cores.max", "6") \
                        .getOrCreate()

        sc = spark.sparkContext
        # using SQLContext to read file
        sqlContext = SQLContext(sc)
        df = sqlContext.read.csv('work/2019-Oct-Nov.csv', header='True')
        df.repartition(1).write.parquet('work/Write outputs/2019-Oct-Nov.parquet')

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)
        print('%\n')
        assert True



    def test_write_dask_1M_parquet(self):
        print('test_write_dask_1M_parquet')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')

        df = dd.read_csv('work/2019-Oct-1M.csv').compute()
        df.to_parquet('work/Write outputs/2019-Oct-1M.parquet')

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)
        print('%\n')

        assert True
    
    def test_write_dask_5M_parquet(self):
        print('test_write_dask_5M_parquet')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        df = dd.read_csv('work/2019-Oct-5M.csv').compute()
        df.to_parquet('work/Write outputs/2019-Oct-5M.parquet')

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)
        print('%\n')

        assert True

    def test_write_dask_10M_parquet(self):
        print('test_write_dask_10M_parquet')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        df = dd.read_csv('work/2019-Oct-10M.csv').compute()
        df.to_parquet('work/Write outputs/2019-Oct-10M.parquet')

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)
        print('%\n')       
        assert True

    def test_write_dask_25M_parquet(self):
        print('test_write_dask_25M_parquet')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        df = dd.read_csv('work/2019-Oct-25M.csv').compute()
        df.to_parquet('work/Write outputs/2019-Oct-25M.parquet')

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)
        print('%\n')       
        assert True

    def test_write_dask_total(self):
        print('test_write_dask_total')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        df = dd.read_csv('work/2019-Oct-Nov.csv').compute()
        df.to_csv('work/Write outputs/2019-Oct-Nov.csv')
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)
        print('%\n')       
        assert True


    def test_write_vaex_1M_parquet(self):
        print('test_write_vaex_1M_parquet')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        df = vaex.open('work/2019-Oct-1M.csv')
        df.export_parquet('work/Write outputs/2019-Oct-1M.parquet')
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       
        assert True

    def test_write_vaex_5M_parquet(self):
        print('test_write_vaex_5M_parquet')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        df = vaex.open('work/2019-Oct-5M.csv')
        df.export_parquet('work/Write outputs/2019-Oct-5M.parquet')
   
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       

        assert True

    def test_write_vaex_10M_parquet(self):
        print('test_write_vaex_10M_parquet')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        df = vaex.open('work/2019-Oct-10M.csv')
        df.export_parquet('work/Write outputs/2019-Oct-10M.parquet')

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       

        assert True   

    def test_write_vaex_25M_parquet(self):
        print('test_write_vaex_25M_parquet')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        df = vaex.open('work/2019-Oct-25M.csv')
        df.export_parquet('work/Write outputs/2019-Oct-25M.parquet')

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       

        assert True        
    
    def test_write_vaex_total_parquet(self):
        print('test_write_vaex_total_parquet')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        df = vaex.open('work/2019-Oct-Nov.csv')
        df.export_parquet('work/Write outputs/2019-Oct-Nov.parquet')
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       

        assert True
 



