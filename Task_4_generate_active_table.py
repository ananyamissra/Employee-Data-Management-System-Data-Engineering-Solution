from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from pyspark.sql.types import DateType
from datetime import datetime

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

active_df=spark.read.jdbc(url=jdbc_url, table='airflow.emp_timeframe_dim', properties=properties)

#filtering employees by Active status
active_df = active_df.filter(col("status") == "Active")

#Group By by designation and counting
active_by_designation = active_df.groupBy("designation").agg(count("designation").alias("count"))

active_by_designation.write \
    .mode("overwrite") \
    .format("csv") \
    .save("s3://ttn-de-bootcamp-2024-bronze-us-east-1/rohan.majhi/active_emp.csv")
    
active_by_designation.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "airflow.active_emp_table") \
    .option("user", "postgres") \
    .option("password", "1234") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()





