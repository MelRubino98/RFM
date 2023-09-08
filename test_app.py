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
        
        
        
    def test_read_pandas_1M(self):
        print('test_read_pandas_1M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')
    
        df = pd.read_csv('work/2019-Oct-1M.csv')
        
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)
        print('%\n')
       
        assert True

    def test_read_pandas_5M(self):
        print('test_read_pandas_5M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')
    
        df = pd.read_csv('work/2019-Oct-5M.csv')
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)
        print('%\n')
        
        assert True
        
    def test_read_pandas_10M(self):
        print('test_read_pandas_10M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')
        
        df = pd.read_csv('work/2019-Oct-10M.csv')

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)
        print('%\n')

        assert True
    
    def test_read_pandas_25M(self):
        print('test_read_pandas_25M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')
    
        df = pd.read_csv('work/2019-Oct-25M.csv')

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)
        print('%\n')

        assert True

    #def test_read_pandas_total(self):
    #    print('test_read_pandas_total')
    #    gc.collect()
    #    print('\nRAM ocupada ANTES del test: ')
    #    print(psutil.virtual_memory().percent)
    #    print('%')

    #    df = pd.read_csv('work/2019-Oct-Nov.csv')

    #    print('\nRAM ocupada DESPUÉS del test: ')
    #    print(psutil.virtual_memory().percent)
    #    print('%\n')
    #    assert True


    
    
    def test_read_dask_1M(self):
        print('test_read_dask_1M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')

        df = dd.read_csv('work/2019-Oct-1M.csv')

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)
        print('%\n')

        assert True
    
    def test_read_dask_5M(self):
        print('test_read_dask_5M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        df = dd.read_csv('work/2019-Oct-5M.csv')

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)
        print('%\n')

        assert True

    def test_read_dask_10M(self):
        print('test_read_dask_10M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        df = dd.read_csv('work/2019-Oct-10M.csv')
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)
        print('%\n')       
        assert True

    def test_read_dask_25M(self):
        print('test_read_dask_25M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        df = dd.read_csv('work/2019-Oct-25M.csv')
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)
        print('%\n')       
        assert True

    def test_read_dask_total(self):
        print('test_read_dask_total')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        df = dd.read_csv('work/2019-Oct-Nov.csv')
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)
        print('%\n')       
        assert True  




    def test_read_dask_compute_1M(self):
        print('test_read_dask_compute_1M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        df = dd.read_csv('work/2019-Oct-1M.csv').compute()
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       
        
        assert True
    
    def test_read_dask_compute_5M(self):
        print('test_read_dask_compute_5M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        df = dd.read_csv('work/2019-Oct-5M.csv').compute()
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       
       
        assert True

    def test_read_dask_compute_10M(self):
        print('test_read_dask_compute_10M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        df = dd.read_csv('work/2019-Oct-10M.csv').compute()
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       
       
        assert True

    def test_read_dask_compute_25M(self):
        print('test_read_dask_compute_25M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        df = dd.read_csv('work/2019-Oct-25M.csv').compute()
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       
       
        assert True

    #def test_read_dask_compute_total(self):
    #    print('test_read_dask_compute_total')
    #    gc.collect()
    #    print('\nRAM ocupada ANTES del test: ')
    #    print(psutil.virtual_memory().percent)
    #    print('%')        
    #    df = dd.read_csv('work/2019-Oct-Nov.csv').compute()
    #    print('\nRAM ocupada DESPUÉS del test: ')
    #    print(psutil.virtual_memory().percent)        
    #    print('%\n')       
       
    #    assert True  





    def test_read_vaex_1M(self):
        print('test_read_vaex_1M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        df = vaex.open('work/2019-Oct-1M.csv')
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       
        assert True

    def test_read_vaex_5M(self):
        print('test_read_vaex_5M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        df = vaex.open('work/2019-Oct-5M.csv')
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       

        assert True

    def test_read_vaex_10M(self):
        print('test_read_vaex_10M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        df = vaex.open('work/2019-Oct-10M.csv')
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       

        assert True   

    def test_read_vaex_25M(self):
        print('test_read_vaex_25M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        df = vaex.open('work/2019-Oct-25M.csv')
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       

        assert True        
    
    def test_read_vaex_total(self):
        print('test_read_vaex_total')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        df = vaex.open('work/2019-Oct-Nov.csv')
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       

        assert True  




    def test_read_pyspark_1M(self):
        print('test_read_pyspark_1M')
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
        
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)
        print('%\n')
       
        assert True

    def test_read_pyspark_5M(self):
        print('test_read_pyspark_5M')
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

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)
        print('%\n')
        
        assert True
        
    def test_read_pyspark_10M(self):
        print('test_read_pyspark_10M')
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

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)
        print('%\n')

        assert True
    
    def test_read_pyspark_25M(self):
        print('test_read_pyspark_25M')
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

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)
        print('%\n')

        assert True

    def test_read_pyspark_total(self):
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

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)
        print('%\n')
        assert True


    def test_describe_pandas_1M(self):
        print('test_describe_pandas_1M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        df = pd.read_csv('work/2019-Oct-1M.csv')
        df_describe = df.describe(include='all')
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       
            
        assert True

    def test_describe_pandas_5M(self):
        print('test_describe_pandas_5M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        df = pd.read_csv('work/2019-Oct-5M.csv')
        df_describe = df.describe(include='all')
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')
        
        assert True

    def test_describe_pandas_10M(self):
        print('test_describe_pandas_10M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        df = pd.read_csv('work/2019-Oct-10M.csv')
        df_describe = df.describe(include='all')
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       
            
        assert True

    def test_describe_pandas_25M(self):
        print('test_describe_pandas_25M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        df = pd.read_csv('work/2019-Oct-25M.csv')
        df_describe = df.describe(include='all')
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       
            
        assert True

    
    def test_describe_pandas_total(self):
        print('test_describe_pandas_total')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        df = pd.read_csv('work/2019-Oct-Nov.csv')
        df_describe = df.describe(include='all')
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       
            
        assert True

    def test_describe_vaex_1M(self):
        print('test_describe_vaex')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')         
        df = vaex.open('work/2019-Oct-1M.csv')
        df_describe = df.describe()
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       
            
        assert True

    def test_describe_vaex_5M(self):
        print('test_describe_vaex')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')         
        df = vaex.open('work/2019-Oct-5M.csv')
        df_describe = df.describe()
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       
            
        assert True
        
    def test_describe_vaex_10M(self):
        print('test_describe_vaex')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')         
        df = vaex.open('work/2019-Oct-10M.csv')
        df_describe = df.describe()
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       
            
        assert True

    def test_describe_vaex_25M(self):
        print('test_describe_vaex')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')         
        df = vaex.open('work/2019-Oct-25M.csv')
        df_describe = df.describe()
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       
            
        assert True

        
    def test_describe_vaex_total(self):
        print('test_describe_vaex_total')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')         
        df = vaex.open('work/2019-Oct-Nov.csv')
        df_describe = df.describe()
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       
            
        assert True


    def test_describe_dask_1M(self):
        print('test_describe_dask')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        df = dd.read_csv('work/2019-Oct-1M.csv')
        df_describe = df.describe(include='all', datetime_is_numeric=True).compute()
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       
            
        assert True

    def test_describe_dask_5M(self):
        print('test_describe_dask')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        df = dd.read_csv('work/2019-Oct-5M.csv')
        df_describe = df.describe(include='all', datetime_is_numeric=True).compute()
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       
            
        assert True

    def test_describe_dask_10M(self):
        print('test_describe_dask_10M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        df = dd.read_csv('work/2019-Oct-10M.csv')
        df_describe = df.describe(include='all', datetime_is_numeric=True).compute()
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       
            
        assert True

    def test_describe_dask_25M(self):
        print('test_describe_dask_25M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        df = dd.read_csv('work/2019-Oct-25M.csv')
        df_describe = df.describe(include='all', datetime_is_numeric=True).compute()
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       
            
        assert True

    def test_describe_dask_total(self):
        print('test_describe_dask_total')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        df = dd.read_csv('work/2019-Oct-Nov.csv')
        df_describe = df.describe(include='all', datetime_is_numeric=True).compute()
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       
            
        assert True
        
    def test_describe_pyspark_1M(self):
        print('test_describe_pyspark')
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
        df = df.describe()
        
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       
            
        assert True

    def test_describe_pyspark_5M(self):
        print('test_describe_pyspark_5M')
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
        df = df.describe()
        
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       
            
        assert True

    def test_describe_pyspark_10M(self):
        print('test_describe_pyspark_10M')
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
        df = df.describe()
        
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       
            
        assert True


    def test_describe_pyspark_25M(self):
        print('test_describe_pyspark_25M')
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
        df = df.describe()
        
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       
            
        assert True


    def test_describe_pyspark_total(self):
        print('test_describe_pyspark_total')
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
        df = df.describe()
        
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       
            
        assert True




    def test_drop_duplicates_pandas_1M(self):
        print('test_drop_duplicates_pandas_1M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
    
        df = pd.read_csv('work/2019-Oct-1M.csv')
        df.drop_duplicates(inplace=True)
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       
            
        assert True

    def test_drop_duplicates_pandas_5M(self):
        print('test_drop_duplicates_pandas_5M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
    
        df = pd.read_csv('work/2019-Oct-5M.csv')
        df.drop_duplicates(inplace=True)
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       
            
        assert True

    def test_drop_duplicates_pandas_10M(self):
        print('test_drop_duplicates_pandas_10M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
    
        df = pd.read_csv('work/2019-Oct-10M.csv')
        df.drop_duplicates(inplace=True)
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       
            
        assert True

    def test_drop_duplicates_pandas_25M(self):
        print('test_drop_duplicates_pandas_25M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
    
        df = pd.read_csv('work/2019-Oct-25M.csv')
        df.drop_duplicates(inplace=True)
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       
            
        assert True  


    def test_drop_duplicates_pandas_total(self):
        print('test_drop_duplicates_pandas_total')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
    
        df = pd.read_csv('work/2019-Oct-Nov.csv')
        df.drop_duplicates(inplace=True)
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       
            
        assert True


    def test_drop_duplicates_dask_1M(self):
        print('test_drop_duplicates_dask_1M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        df = dd.read_csv('work/2019-Oct-1M.csv')
        df.drop_duplicates()
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
        df.drop_duplicates()
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
        df.drop_duplicates()
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
        df.drop_duplicates()
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
        df.drop_duplicates()
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       

        assert True


    def test_drop_duplicates_pyspark_1M(self):
        print('test_drop_duplicates_pyspark_1M')
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
        df = df.dropDuplicates()

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       

        assert True

    def test_drop_duplicates_pyspark_5M(self):
        print('test_drop_duplicates_pyspark_5M')
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
        df = df.dropDuplicates()

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       

        assert True
 
    def test_drop_duplicates_pyspark_10M(self):
        print('test_drop_duplicates_pyspark_10M')
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
        df = df.dropDuplicates()

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       

        assert True
 
    def test_drop_duplicates_pyspark_25M(self):
        print('test_drop_duplicates_pyspark_25M')
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
        df = df.dropDuplicates()

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       

        assert True
      

    def test_drop_duplicates_pyspark_total(self):
        print('test_drop_duplicates_pyspark_total')
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
        df = df.dropDuplicates()

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       

        assert True
 



    def test_filter_pandas_1M(self):
        print('filter_pandas_1M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        

        df = pd.read_csv('work/2019-Oct-1M.csv')
        df = df[df['event_type']=='purchase']

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       

        assert True   

    def test_filter_pandas_5M(self):
        print('filter_pandas_5M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        

        df = pd.read_csv('work/2019-Oct-5M.csv')
        df = df[df['event_type']=='purchase']
        
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       

        assert True   

    def test_filter_pandas_10M(self):
        print('filter_pandas_10M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        

        df = pd.read_csv('work/2019-Oct-10M.csv')
        df = df[df['event_type']=='purchase']
        
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       

        assert True   

    def test_filter_pandas_25M(self):
        print('filter_pandas_25M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        

        df = pd.read_csv('work/2019-Oct-25M.csv')
        df = df[df['event_type']=='purchase']
        
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       

        assert True 

    def test_filter_pandas_total(self):
        print('filter_pandas_total')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        

        df = pd.read_csv('work/2019-Oct-Nov.csv')
        df = df[df['event_type']=='purchase']
        
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       

        assert True 
 
       


    def test_filter_dask_1M(self):
        print('filter_dask_1M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        

        df = dd.read_csv('work/2019-Oct-1M.csv')
        df = df.loc[(df["event_type"] == 'purchase')]
        
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       

        assert True   

    def test_filter_dask_5M(self):
        print('filter_dask_5M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        

        df = dd.read_csv('work/2019-Oct-5M.csv')
        df = df.loc[(df["event_type"] == 'purchase')]
        
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       
        assert True   

    def test_filter_dask_10M(self):
        print('filter_dask_10M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        

        df = dd.read_csv('work/2019-Oct-10M.csv')
        df = df.loc[(df["event_type"] == 'purchase')]
        
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       
        assert True   

    def test_filter_dask_25M(self):
        print('filter_dask_25M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        

        df = dd.read_csv('work/2019-Oct-25M.csv')
        df = df.loc[(df["event_type"] == 'purchase')]
        
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       
        assert True 

    def test_filter_dask_total(self):
        print('filter_dask_total')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        

        df = dd.read_csv('work/2019-Oct-Nov.csv')
        df = df.loc[(df["event_type"] == 'purchase')]
        
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       
        assert True 




    def test_filter_vaex_1M(self):
        print('filter_vaex_1M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        

        df = vaex.open('work/2019-Oct-1M.csv')
        df = df[(df["event_type"] == 'purchase')]  
        
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       
      
        assert True   

    def test_filter_vaex_5M(self):
        print('filter_vaex_5M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        

        df = vaex.open('work/2019-Oct-5M.csv')
        df = df[(df["event_type"] == 'purchase')]  
        
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')          
        assert True   

    def test_filter_vaex_10M(self):
        print('filter_vaex_10M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        

        df = vaex.open('work/2019-Oct-10M.csv')
        df = df[(df["event_type"] == 'purchase')]  
        
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')              
        assert True   

    def test_filter_vaex_25M(self):
        print('filter_vaex_25M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        

        df = vaex.open('work/2019-Oct-25M.csv')
        df = df[(df["event_type"] == 'purchase')]  
        
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')                
        assert True 

    def test_filter_vaex_total(self):
        print('filter_vaex_total')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        

        df = vaex.open('work/2019-Oct-Nov.csv')
        df = df[(df["event_type"] == 'purchase')]  
        
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')          
        assert True 


    def test_filter_pyspark_1M(self):
        print('test_filter_pyspark_1M')
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
        df=df.filter((df['event_type']=='purchase'))
        
        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)
        print('%\n')
       
        assert True

    def test_filter_pyspark_5M(self):
        print('test_filter_pyspark_5M')
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
        df=df.filter((df['event_type']=='purchase'))

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)
        print('%\n')
        
        assert True
        
    def test_filter_pyspark_10M(self):
        print('test_filter_pyspark_10M')
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
        df = df.filter((df['event_type']=='purchase'))

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)
        print('%\n')

        assert True
    
    def test_filter_pyspark_25M(self):
        print('test_filter_pyspark_25M')
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
        df=df.filter((df['event_type']=='purchase'))

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)
        print('%\n')

        assert True

    def test_filter_pyspark_total(self):
        print('test_filter_pyspark_total')
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
        df = df.filter((df['event_type']=='purchase'))

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)
        print('%\n')
        assert True



    def test_del_column_pandas_1M(self):
        print('del_column_pandas_1M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        

        df = pd.read_csv('work/2019-Oct-1M.csv')
        df = df.drop(columns=['user_session'])

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')          

        assert True   

    def test_del_column_pandas_5M(self):
        print('del_column_pandas_5M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        

        df = pd.read_csv('work/2019-Oct-5M.csv')
        df = df.drop(columns=['user_session'])

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')          
        
        assert True   

    def test_del_column_pandas_10M(self):
        print('del_column_pandas_10M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        

        df = pd.read_csv('work/2019-Oct-10M.csv')
        df = df.drop(columns=['user_session'])

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')          
        
        assert True   

    def test_del_column_pandas_25M(self):
        print('del_column_pandas_25M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        

        df = pd.read_csv('work/2019-Oct-25M.csv')
        df = df.drop(columns=['user_session'])

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')          
        
        assert True 

    def test_del_column_pandas_total(self):
        print('del_column_pandas_total')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        

        df = pd.read_csv('work/2019-Oct-Nov.csv')
        df = df.drop(columns=['user_session'])

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')          
        assert True 





    def test_del_column_dask_1M(self):
        print('del_column_dask_1M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        
        df = dd.read_csv('work/2019-Oct-1M.csv')
        df = df.drop(columns=['user_session'])

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')          

        assert True   

    def test_del_column_dask_5M(self):
        print('del_column_dask_5M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        
        df = dd.read_csv('work/2019-Oct-5M.csv')
        df = df.drop(columns=['user_session'])

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')          
        assert True   

    def test_del_column_dask_10M(self):
        print('del_column_dask_10M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        
        df = dd.read_csv('work/2019-Oct-10M.csv')
        df = df.drop(columns=['user_session'])

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')          
        assert True   

    def test_del_column_dask_25M(self):
        print('del_column_dask_25M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        
        df = dd.read_csv('work/2019-Oct-25M.csv')
        df = df.drop(columns=['user_session'])

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')          
        assert True 

    def test_del_column_dask_total(self):
        print('del_column_dask_total')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        
        
        df = dd.read_csv('work/2019-Oct-Nov.csv')
        df = df.drop(columns=['user_session'])

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')          
        assert True 



    def test_del_col_vaex_1M(self):
        print('del_col_vaex_1M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        

        df = vaex.open('work/2019-Oct-1M.csv')
        df.drop('user_session', inplace=True)  

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')          
       
        assert True   


    def test_del_col_vaex_5M(self):
        print('del_col_vaex_5M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        

        df = vaex.open('work/2019-Oct-5M.csv')
        df.drop('user_session', inplace=True)  

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')          
        assert True   

    def test_del_col_vaex_10M(self):
        print('del_col_vaex_10M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        

        df = vaex.open('work/2019-Oct-10M.csv')
        df.drop('user_session', inplace=True)  

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')          
        assert True   

    def test_del_col_vaex_25M(self):
        print('del_col_vaex_25M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        

        df = vaex.open('work/2019-Oct-25M.csv')
        df.drop('user_session', inplace=True)  

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')          
        assert True 

    def test_del_col_vaex_total(self):
        print('del_col_vaex_total')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        

        df = vaex.open('work/2019-Oct-Nov.csv')
        df.drop('user_session', inplace=True)  

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')          
        assert True 
    
    def test_del_column_pyspark_1M(self):
        print('del_column_pyspark_1M')
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
        df = df.drop(('user_session'))

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')          

        assert True   

    def test_del_column_pyspark_5M(self):
        print('del_column_pyspark_5M')
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
        df = df.drop(('user_session'))


        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')          
        
        assert True   

    def test_del_column_pyspark_10M(self):
        print('del_column_pyspark_10M')
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
        df = df.drop(('user_session'))


        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')          
        
        assert True   

    def test_del_column_pyspark_25M(self):
        print('del_column_pyspark_25M')
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
        df = df.drop(('user_session'))


        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')          
        
        assert True 

    def test_del_column_pyspark_total(self):
        print('del_column_pyspark_total')
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
        df = df.drop(('user_session'))


        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')          
        assert True 

    
 
#pytest -s --durations=0 C:\Users\melanie.g.rubino\Documents\Tesis\Code\test_app.py::Test::test_read_pandas
    
#pytest -s test_apps.py > logs/test_apps$(date '+%Y%m%d%H%M%S').log
#pytest -s --durations=0 C:\Users\melanie.g.rubino\Documents\Tesis\Code\test_app.py > logs/test_%time:~0,2%%time:~3,2%%time:~6,2%_%date:~-10,2%%date:~-7,2%%date:~-4,4%.log 


    def test_groupby_pandas_1M(self):
        print('groupby_pandas_1M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        

        df = pd.read_csv('work/2019-Oct-1M.csv')
        df.groupby(by='user_id', as_index=False)['price'].sum()  

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')          
       
        assert True   


    def test_groupby_pandas_5M(self):
        print('groupby_pandas_5M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        

        df = pd.read_csv('work/2019-Oct-5M.csv')
        df.groupby(by='user_id', as_index=False)['price'].sum()  

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')          
        assert True   

    def test_groupby_pandas_10M(self):
        print('groupby_pandas_10M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        

        df = pd.read_csv('work/2019-Oct-10M.csv')
        df.groupby(by='user_id', as_index=False)['price'].sum()  

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')          
        assert True   

    def test_groupby_pandas_25M(self):
        print('groupby_pandas_25M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        

        df = pd.read_csv('work/2019-Oct-25M.csv')
        df.groupby(by='user_id', as_index=False)['price'].sum()  

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')          
        assert True 

    def test_groupby_pandas_total(self):
        print('groupby_pandas_total')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        

        df = pd.read_csv('work/Original\2019-Oct-Nov.csv')
        df.groupby(by='user_id', as_index=False)['price'].sum()  

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')          
        assert True 



        
    def test_groupby_dask_1M(self):
        print('groupby_dask_1M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        

        df = dd.read_csv('work/2019-Oct-1M.csv')
        df.groupby('user_id').agg({'price':['sum']}).reset_index()  

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')          
       
        assert True   


    def test_groupby_dask_5M(self):
        print('groupby_dask_5M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        

        df = dd.read_csv('work/2019-Oct-5M.csv')
        df.groupby('user_id').agg({'price':['sum']}).reset_index()  

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')          
        assert True   

    def test_groupby_dask_10M(self):
        print('groupby_dask_10M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        

        df = dd.read_csv('work/2019-Oct-10M.csv')
        df.groupby('user_id').agg({'price':['sum']}).reset_index()  

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')          
        assert True   

    def test_groupby_dask_25M(self):
        print('groupby_dask_25M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        

        df = dd.read_csv('work/2019-Oct-25M.csv')
        df.groupby('user_id').agg({'price':['sum']}).reset_index()  

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')          
        assert True 

    def test_groupby_dask_total(self):
        print('groupby_dask_total')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        

        df = dd.read_csv('work/Original\2019-Oct-Nov.csv')
        df.groupby('user_id').agg({'price':['sum']}).reset_index()  

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')          
        assert True 

        


        
    def test_groupby_vaex_1M(self):
        print('groupby_vaex_1M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        

        df = vaex.read_csv('work/2019-Oct-1M.csv')
        df.groupby(by=['user_id'], agg=({'price': 'sum'}))  

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')          
       
        assert True   


    def test_groupby_vaex_5M(self):
        print('groupby_vaex_5M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        

        df = vaex.read_csv('work/2019-Oct-5M.csv')
        df.groupby(by=['user_id'], agg=({'price': 'sum'}))  

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')          
        assert True   

    def test_groupby_vaex_10M(self):
        print('groupby_vaex_10M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        

        df = vaex.read_csv('work/2019-Oct-10M.csv')
        df.groupby(by=['user_id'], agg=({'price': 'sum'}))  

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')          
        assert True   

    def test_groupby_vaex_25M(self):
        print('groupby_vaex_25M')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        

        df = vaex.read_csv('work/2019-Oct-25M.csv')
        df.groupby(by=['user_id'], agg=({'price': 'sum'}))  

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')          
        assert True 

    def test_groupby_vaex_total(self):
        print('groupby_vaex_total')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')        

        df = vaex.read_csv('work/Original\2019-Oct-Nov.csv')
        df.groupby(by=['user_id'], agg=({'price': 'sum'}))  

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')          
        assert True 


    def test_groupby_pyspark_1M(self):
        print('groupby_pyspark_1M')
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
        df = df.groupBy("user_id").agg({"price":"sum"})

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')          

        assert True   

    def test_groupby_pyspark_5M(self):
        print('groupby_pyspark_5M')
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
        df = df.groupBy("user_id").agg({"price":"sum"})


        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')          
        
        assert True   

    def test_groupby_pyspark_10M(self):
        print('groupby_pyspark_10M')
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
        df = df.groupBy("user_id").agg({"price":"sum"})


        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')          
        
        assert True   

    def test_groupby_pyspark_25M(self):
        print('groupby_pyspark_25M')
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
        df = df.groupBy("user_id").agg({"price":"sum"})


        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')          
        
        assert True 

    def test_groupby_pyspark_total(self):
        print('groupby_pyspark_total')
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
        df = df.groupBy("user_id").agg({"price":"sum"})


        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')          
        assert True 



        

 
