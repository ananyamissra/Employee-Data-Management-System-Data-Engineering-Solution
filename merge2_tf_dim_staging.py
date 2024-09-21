from pyspark.sql import SparkSession
#import boto3
from pyspark.sql import functions as F
from pyspark.sql.window import Window
#from io import StringIO
#import pandas as pd
from pyspark.sql.functions import col, to_date,min

driver_path = '/usr/lib/spark/jars/postgresql-42.7.3.jar'

spark = SparkSession.builder \
    .appName("RDS Access") \
    .config("spark.jars",driver_path) \
    .config('spark.network.timeout','10000000')\
    .config("spark.log4jHotPatch.enabled", "false") \
    .getOrCreate()

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

jdbc_url='jdbc:postgresql://ec2-3-94-106-128.compute-1.amazonaws.com:5432/postgres'
properties = {
    "user": "postgres",
    "password": "1234",
    "driver": "org.postgresql.Driver"
}

ab= spark.read.jdbc(url=jdbc_url, table='airflow.emp_timeframe_dim', properties=properties)
ab.printSchema()
ab.show(n=10)
ab_new= spark.read.jdbc(url=jdbc_url, table='airflow.emp_timeframe_staging', properties=properties)
ab_new.printSchema()
ab_new_2 = ab_new.select('*').groupBy(ab_new.emp_id).agg(min(ab_new.start_date)) \
    .withColumn("dum_status", F.lit("Inactive"))

# Using new df instead of new_data(ab_new)
ab_new_2 = ab_new_2.withColumnRenamed("emp_id", "e_id")

# Join ab with ab_new_2
cond = ((ab["emp_id"] == ab_new_2["e_id"]) & ((ab.end_date).isNull()))

# Updation
new_df = ab.alias("s").join(ab_new_2.alias("t"), cond, "left") \
    .withColumn("end_date", F.coalesce("end_date", "min(start_date)")) \
    .withColumn("status", F.coalesce("dum_status", "status")) \
    .drop("e_id", "min(start_date)", "dum_status")

# Inserting new rows
new_df = new_df.union(ab_new)

new_df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://ec2-3-94-106-128.compute-1.amazonaws.com:5432/postgres") \
    .option("dbtable", "airflow.emp_timeframe_dim") \
    .option("user", "postgres") \
    .option("password", "1234") \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()