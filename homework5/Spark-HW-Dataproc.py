#!/usr/bin/env python
# coding: utf-8

import pyspark
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql import types
import argparse

parser = argparse.ArgumentParser()

parser.add_argument('--input_fhv', required=True)
parser.add_argument('--input_zones', required=True)
parser.add_argument('--gcs_output', required=True)

args = parser.parse_args()

input_fhv = args.input_fhv
input_zones = args.input_zones
gcs_output = args.gcs_output

spark = SparkSession.builder \
    .appName("Spark HW Local") \
    .getOrCreate()

spark.conf.set('temporaryGcsBucket', 'dataproc-temp-us-central1-927229007396-9k1xvwvm')

# The output of spark.version.
print(f'Spark version is {spark.version}')

df_spark = spark.read \
    .option('header', 'true') \
    .option('inferschema', 'true') \
    .csv(input_fhv)

# df_spark.printSchema()
# df_spark.schema

"""
schema = types.StructType([
    types.StructField('dispatching_base_num', types.StringType(), True), 
    types.StructField('pickup_datetime', types.TimestampType(), True), 
    types.StructField('dropOff_datetime', types.TimestampType(), True), 
    types.StructField('PUlocationID', types.IntegerType(), True), 
    types.StructField('DOlocationID', types.IntegerType(), True), 
    types.StructField('SR_Flag', types.StringType(), True), 
    types.StructField('Affiliated_base_number', types.StringType(), True)
])
"""

"""
df_spark = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv("data/raw/fhv_tripdata_2019-10.csv")
"""

df_spark = df_spark.repartition(6)

df_spark.write.parquet(f'{gcs_output}/fhv10/', mode='overwrite')

df_spark.createOrReplaceTempView('fhv_tripdata')

# Number of trips on the 15th of October

print("Number of trips on the 15th of October:")
spark.sql(
"""
SELECT
    COUNT(1) AS cctober_15_trips
FROM
    fhv_tripdata
WHERE
    DATE(pickup_datetime)='2019-10-15'
"""
).show()

# The longest trip in the dataset in hours
# TIMESTAMPDIFF(HOUR, ... also works, but it returns 0 for the value after the decimal point

print("The longest trip in the dataset in hours:")
spark.sql(
"""
SELECT
    pickup_datetime,
    dropOff_datetime,
    TIMESTAMPDIFF(SECOND, pickup_datetime, dropOff_datetime)/3600.0 AS longest_trip
FROM
    fhv_tripdata
ORDER BY
    longest_trip DESC
LIMIT 1
"""
).show()

df_zones= spark.read.parquet(input_zones)
df_zones.createOrReplaceTempView('zones')

# Least frequent pickup location zone

print("The five Least frequent pickup location zones:")
spark.sql(
"""
SELECT
    COUNT(1) AS trips_per_pickup_zone,
    zones.Zone AS pickup_zone
FROM
    fhv_tripdata
JOIN
    zones ON fhv_tripdata.PUlocationID = zones.LocationID
GROUP BY
    zones.Zone
ORDER BY
    trips_per_pickup_zone ASC
LIMIT 5;
""" 
).show()