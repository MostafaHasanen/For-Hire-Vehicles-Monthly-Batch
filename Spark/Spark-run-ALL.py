#!/usr/bin/env python
# coding: utf-8
import argparse
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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
parser.add_argument('--year_month', required=True)
parser.add_argument('--year', required=True)
args = parser.parse_args()

exe_date = args.year_month
year = args.year
FILE_TEMPLATE = f'gs://dtc_data_lake_de-zoom-359609/raw/{year}/fhvhv_{exe_date}.parquet'

spark = SparkSession.builder \
    .appName('Spark/ling') \
    .getOrCreate()
#========================================================================================================
#Break Point: What to do!: 1- Add table to Bigquery to be able to work on dbt 2-Orchistera this Airflow!
###
spark.conf.set('temporaryGcsBucket', 'dataproc-temp-europe-west1-415448054083-dwrpbx9g')

s_df = spark.read \
    .schema(schema) \
    .parquet(FILE_TEMPLATE)
    
s_df = s_df.drop("hvfhs_license_numtypes") # All Column is NULL; date: 10/1/2023 %D %M %Y

s_df.createOrReplaceTempView('fhvhv_table_temp')

df_result = spark.sql("""
SELECT 
    *
FROM
    fhvhv_table_temp
""")

###
df_result.write.format('bigquery') \
    .option('table', "ForHireVehiclesMAIN_1stApproach.FHVHV_ALL_Data") \
    .partitionBy("pickup_datetime") \
    .mode("append") \
    .save()

