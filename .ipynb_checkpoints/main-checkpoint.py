from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, Row, ArrayType
from pyspark.sql import SQLContext
from pyspark.sql.functions import from_json, explode, col, udf
from pyspark.sql import functions as F
import requests
import os
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine as ce
import datetime
import json

spark = SparkSession.builder\
.master('local')\
.appName('SportifyTracks')\
.getOrCreate()
sql_context = SQLContext(spark)

load_dotenv()

def convert_date(date):
    dt = date.split('T')[0]
    return dt

def extract():
    df = spark.read.json('./tracks.json', multiLine=True)
    return df


def transform(df):
    convert_date_udf = udf(lambda z : convert_date(z), StringType())
    sql_context.udf.register('convertGender', convert_date_udf)
    
    dataframe = df.withColumn('col', explode('items'))\
    .withColumn('track_name', col('col.track.name'))\
    .withColumn('col2', explode('col.track.artists'))\
    .withColumn('artist', col('col2.name'))\
    .withColumn('played_at', col('col.played_at'))\
    .drop('cursors', 'href', 'limit', 'next', 'items', 'col','col2')
    
    
    df_transformed = dataframe.dropDuplicates(['track_name']).select('track_name', 'artist', convert_date_udf('played_at').alias('played_at')).sort('played_at')
    return df_transformed
    

def load(url, df):
    engine = ce(url, echo=True)
    connection = engine.connect()
    
    try:
        df.to_sql("my_played_tracks", engine, index=False, if_exists='append')
    except:
         print("Data already exists in the database")

    connection.close()
    print("Close database successfully")
    
if __name__ == '__main__':
    url = 'mysql+pymysql://root:admin@172.17.0.3/my_tracks'
    df = extract()
    df.printSchema()
    df_transformed = transform(df)
    
    load(url, df_transformed.toPandas())

###
# Mysql Database
# docker run -d --rm --name mysql -e MYSQL_ROOT_PASSWORD=admin -p 3306:3306 -v /Users/root1/Documents/practice/spark/sporify-etl/database:/var/lib/mysql mysql:8.0
###