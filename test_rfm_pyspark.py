# -*- coding: utf-8 -*-
"""
Created on Thu Feb  2 18:37:35 2023

@author: melanie.g.rubino
"""

import numpy as np
import pandas as pd
import os
import datetime
from datetime import datetime, timedelta
import gc
import psutil

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import datediff, to_date, lit, col, max as max_
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from itertools import chain
from pyspark.sql.functions import create_map, lit


class Test:
    def test_directorio(self):
        print('Directorio: ', os.listdir())
        assert True

    def test_rfm_pyspark(self):
        print('test_rfm_pyspark')
        gc.collect()
        print('\nRAM ocupada ANTES del test: ')
        print(psutil.virtual_memory().percent)
        print('%')         


        spark = SparkSession.builder \
            .master('local') \
            .appName('myAppName') \
            .config('spark.executor.memory', '5gb') \
            .config("spark.cores.max", "6") \
            .getOrCreate()
        
        sc = spark.sparkContext
        sqlContext = SQLContext(sc)
        
        df = sqlContext.read.csv('work/RFM/2019-Oct-Nov-transformed.csv', header='True')
        
        df = df.select(to_date(df.event_time, 'yyyy-MM-dd').alias('event_time'), 
                     df.event_type, df.product_id, df.category_code, df.brand, df.price, df.user_id)
        
        #RECENCY
        window= np.datetime64('2019-12-01','ns')
        window = window.astype("datetime64[D]")
        df_recency = (df.withColumn("event_time", col("event_time").cast("timestamp"))
            .groupBy("user_id")
            .agg(max_("event_time")))
        df_recency = df_recency.withColumn("Recency", 
                      datediff(to_date(lit("2019-12-01")),
                               to_date("max(event_time)","yyyy/MM/dd")))
        df_recency = df_recency.drop('max(event_time)')
        
        
        #FREQUENCY
        df_frequency = df.dropDuplicates()
        df_frequency = df_frequency.groupBy("user_id").count()
        df_frequency=df_frequency.withColumnRenamed("count","Frequency")
        
        
        #MONETARY
        df_monetary = df.groupBy("user_id").agg({"price":"sum"})
        df_monetary=df_monetary.withColumnRenamed("sum(price)","Monetary")
        
        
        #MERGE DE LOS TRES DF
        df_rfm = df_recency.join(df_frequency, how='inner', on='user_id')
        df_rfm = df_rfm.join(df_monetary, how='inner', on='user_id')
        
        #ELIMINAMOS OUTLIERS UTILIZANDO EL PERCENTIL 98
        #Chequeamos máx y min de Recency
        print(df_rfm.agg({"Recency": "max"}).collect()[0][0])
        print(df_rfm.agg({"Recency": "min"}).collect()[0][0])
        #Chequeamos máx y mín de Frequency
        print(df_rfm.agg({"Frequency": "max"}).collect()[0][0])
        print(df_rfm.agg({"Frequency": "min"}).collect()[0][0])
        #Chequeamos máx y mín para Monetary
        print(df_rfm.agg({"Monetary": "max"}).collect()[0][0])
        print(df_rfm.agg({"Monetary": "min"}).collect()[0][0])
        
        #Para Frequency, calculamos el percentil 98 (q98) y nos quedamos con los valores menores a él.
        p98 = df_rfm.approxQuantile("Frequency", [0.98], 0)[0]
        df_rfm = df_rfm.filter(df_rfm["Frequency"] <= p98)
        
        #Para Monetary, calculamos el percentil 2 y el percentil 98 (p2 y p98) y nos quedamos con el intervalo entre estos dos valores.
        p2 = df_rfm.approxQuantile("Monetary", [0.02], 0)[0]
        p98 = df_rfm.approxQuantile("Monetary", [0.98], 0)[0]
        df_rfm = df_rfm.filter((df_rfm["Monetary"] <= p98) & (df_rfm["Monetary"] >= p2))
        
        #ASIGNAMOS QUINTILES
        
        #Asignamos quintiles a Recency
        rfm = df_rfm.select("user_id","Recency","Frequency",'Monetary', F.ntile(5).over(Window.partitionBy().orderBy(df_rfm['Recency'])).alias("R_")) 
        #Remapeamos los números para que los 1 sean 5, 2 sean 4, etc.
        simple_dict = {1:5, 2:4, 3:3, 4:2, 5:1}
        mapping_expr = create_map([lit(x) for x in chain(*simple_dict.items())])
        rfm =rfm.withColumn('R', mapping_expr[rfm['R_']])
        rfm = rfm.drop('R_')
        #Para Frequency
        simple_dict = {1:1, 2:1, 3:2, 4:2, 5:3, 6:3, 7:4, 8:4, 9:5,10:5, 11:5}
        mapping_expr = create_map([lit(x) for x in chain(*simple_dict.items())])
        rfm = rfm.withColumn('F', mapping_expr[rfm['Frequency']])
        #Para Monetary
        rfm.collect()
        rfm = rfm.select("user_id","Recency","Frequency",'Monetary',"R","F", F.ntile(5).over(Window.partitionBy().orderBy(df_rfm['Monetary'])).alias("M"))
        
        rfm=rfm.select('user_id','Recency','Frequency','R','F','M',concat(rfm.R, rfm.F, rfm.M).alias("RFM_SCORE"))
        
        #A PARTIR DEL RFM_SCORE MAPEAMOS LOS SEGMENTOS
        seg_map = sqlContext.read.csv('work/RFM/segmentos_rfm.csv', header='True')
        seg_map = seg_map.drop("Value")
        rfm = rfm.join(seg_map, how='inner', on='RFM_SCORE')
        
        #EXPORTAMOS EL DATASET
        rfm.repartition(1).write.csv('work/RFM/RFM PYSPARK Resultados.csv')

        print('\nRAM ocupada DESPUÉS del test: ')
        print(psutil.virtual_memory().percent)        
        print('%\n')       

    assert True



