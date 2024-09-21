from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import DateType
from datetime import datetime,time

driver_path = '/usr/lib/spark/jars/postgresql-42.7.3.jar'

spark = SparkSession.builder \
    .appName("RDS Access") \
    .config("spark.jars", driver_path) \
    .config('spark.network.timeout', '10000000') \
    .config("spark.log4jHotPatch.enabled", "false") \
    .getOrCreate()

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

jdbc_url = 'jdbc:postgresql://ec2-3-94-106-128.compute-1.amazonaws.com:5432/postgres'
properties = {
    "user": "postgres",
    "password": "1234",
    "driver": "org.postgresql.Driver"
}


# Read the data from the tables into DataFrames
leave_quota_df = spark.read.jdbc(
    url='jdbc:postgresql://ec2-3-94-106-128.compute-1.amazonaws.com:5432/postgres',
    table='airflow.leave_quota_dim',
    properties={
        'user': 'postgres',
        'password': '1234',
        'driver': 'org.postgresql.Driver'
    }
)

leave_data_df = spark.read.jdbc(
    url='jdbc:postgresql://ec2-3-94-106-128.compute-1.amazonaws.com:5432/postgres',
    table='airflow.leave_data_dim',
    properties={
        'user': 'postgres',
        'password': '1234',
        'driver': 'org.postgresql.Driver'
    }
)

current_year = datetime.now().year
# CTE 1: Calculate leave available for the current year
cte_df = leave_quota_df.filter(col("leave_year") == current_year) \
    .groupBy("emp_id") \
    .agg(sum("leave_quota").alias("leave_available")) \
    .orderBy("emp_id")

# CTE 2: Calculate leave availed for the current year
cte2_df = leave_data_df.filter(col("status") == "ACTIVE") \
    .groupBy("emp_id") \
    .agg(count("status").alias("leave_availed"))

# Join the CTEs on emp_id
joined_df = cte_df.join(cte2_df, cte_df.emp_id == cte2_df.emp_id, "inner") \
    .select(cte_df.emp_id, "leave_available", "leave_availed")

# Calculate the percentage of leave availed and filter based on the condition
result_df = joined_df.withColumn(
    "percentage",
    round((col("leave_availed") / col("leave_available")) * 100, 2)
).filter(col("percentage") > 80).orderBy("percentage")


# Write the result to PostgreSQL
result_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "airflow.emp_80_percent_leave") \
    .option("user", "postgres") \
    .option("password", "1234") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

#writing to s3 as well
result_df.write \
    .mode("overwrite") \
    .format("csv") \
    .save("s3://ttn-de-bootcamp-2024-bronze-us-east-1/rohan.majhi/emp_80_percent_leave.csv")
