#!/usr/bin/env python
# coding: utf-8
import argparse
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.conf import SparkConf
from pyspark.context import SparkContext

from pyspark.sql import types # Get constant schema for smooth table concat
schema = types.StructType(
[ types.StructField('hvfhs_license_numtypes', types.StringType(), True)
, types.StructField('dispatching_base_num', types.StringType(), True)
, types.StructField('originating_base_num', types.StringType(), True)
, types.StructField('request_datetime', types.TimestampType(), True)
, types.StructField('on_scene_datetime', types.TimestampType(), True)
, types.StructField('pickup_datetime', types.TimestampType(), True)
, types.StructField('dropoff_datetime', types.TimestampType(), True)
, types.StructField('PULocationID', types.LongType(), True)
, types.StructField('DOLocationID', types.LongType(), True)
, types.StructField('trip_miles', types.DoubleType(), True)
, types.StructField('trip_time', types.LongType(), True)
, types.StructField('base_passenger_fare', types.DoubleType(), True)
, types.StructField('tolls', types.DoubleType(), True)
, types.StructField('bcf', types.DoubleType(), True)
, types.StructField('sales_tax', types.DoubleType(), True)
, types.StructField('congestion_surcharge', types.DoubleType(), True)
, types.StructField('airport_fee', types.DoubleType(), True)
, types.StructField('tips', types.DoubleType(), True)
, types.StructField('driver_pay', types.DoubleType(), True)
, types.StructField('shared_request_flag', types.StringType(), True)
, types.StructField('shared_match_flag', types.StringType(), True)
, types.StructField('access_a_ride_flag', types.StringType(), True)
, types.StructField('wav_request_flag', types.StringType(), True)
, types.StructField('wav_match_flag', types.StringType(), True)])

parser = argparse.ArgumentParser()
parser.add_argument('--FILE_TEMPLATE', required=True)
args = parser.parse_args()

FILE_TEMPLATE = args.FILE_TEMPLATE

credentials = './codes/lib/.gc/7asanen999_Key_SA.json'
#Config Spark: Hadoop connectors downloaded
    #.setMaster('local[*]') \
Spark_conf = SparkConf() \
    .setAppName('Spark/ling') \
    .set("spark.jars", "./codes/lib/gcs-connector-hadoop2-latest.jar") \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials)
spark_C = SparkContext(conf=Spark_conf)
hadoopConf = spark_C._jsc.hadoopConfiguration()
# 
hadoopConf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoopConf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoopConf.set("fs.gs.auth.service.account.json.keyfile", credentials)
hadoopConf.set("fs.gs.auth.service.account.enable", "true")

spark = SparkSession.builder \
    .config(conf=spark_C.getConf()) \
    .getOrCreate()
#========================================================================================================
spark.conf.set('temporaryGcsBucket', 'dataproc-temp-europe-west1-415448054083-dwrpbx9g')

s_df = spark.read \
    .schema(schema) \
    .parquet(FILE_TEMPLATE)

s_df.drop("hvfhs_license_numtypes") # All Column is NULL; date: 10/1/2023 %D %M %Y

s_df.createOrReplaceTempView('fhvhv_table_temp')

### Add partitioning and grouping!!!
df_result = spark.sql("""
SELECT 
    *
FROM
    fhvhv_table_temp
""")

###
# allow tables to be unique in big table for faster queries
# ease in search (36 unique value)
# To have all data in single table added monthly
df_result.write.format('bigquery') \
    .option('table', "ForHireVehiclesMAIN_2ndApproach.FHVHV_ALL_Data") \
    .partitionBy("pickup_datetime") \
    .mode("append") \
    .save()

