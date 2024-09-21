from pyspark.sql import SparkSession
import boto3
from pyspark.sql import functions as F
from pyspark.sql.window import Window
#from io import StringIO
#import pandas as pd
import os

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


table_name = "airflow.emp_timeframe_staging"

s3_client = boto3.client('s3')

# Read CSV file from S3 directly into a DataFrame
bucket_name = 'ttn-de-bootcamp-2024-bronze-us-east-1'
folder_prefix = 'rohan.majhi/Project/Employee_timeframe_data/'

# List objects in the S3 folder
response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)

# Extract file names and last modified timestamps
files = [(obj['Key'], obj['LastModified']) for obj in response.get('Contents', [])]

# Sort files by last modified timestamp (newest first)
files.sort(key=lambda x: x[1], reverse=True)

# Get the newest file
newest_file_key, _ = files[0]

#Read the contents of the newest file
#csv_object = s3_client.get_object(Bucket=bucket_name, Key=newest_file_key)
#csv_data = csv_object['Body'].read().decode('utf-8')

#ead CSV data into a Pandas DataFrame
#pandas_df = pd.read_csv(StringIO(csv_data))

#Convert Pandas DataFrame to Spark DataFrame
#ab = spark.createDataFrame(pandas_df)

s3_path = f"s3://{bucket_name}/{newest_file_key}"
ab=spark.read.csv(path=s3_path,header=True,inferSchema=True)

# # Use the Hadoop filesystem API to list files in the S3 bucket
# fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
# path = spark._jvm.org.apache.hadoop.fs.Path(f"s3a://{bucket_name}/{folder_prefix}")
# files = fs.listStatus(path)

# # Extract file paths and modification times
# files_info = [(f.getPath().toString(), f.getModificationTime()) for f in files if f.isFile()]

# # Sort files by modification time and get the latest file
# latest_file = sorted(files_info, key=lambda x: x[1], reverse=True)[0][0]

# # Read the latest file into a DataFrame
# ab = spark.read.csv(latest_file, header=True, inferSchema=True)

ab = ab.withColumn('start_date', F.from_unixtime(F.col('start_date').cast('bigint')))
ab = ab.withColumn('end_date', F.from_unixtime(F.col('end_date').cast('decimal')))

ab = ab.withColumn('status', F.when(ab.end_date.isNull(), 'Active').otherwise('Inactive'))

windowSpec = Window.partitionBy(ab["emp_id"], ab["end_date"]).orderBy(ab["salary"].desc())

ab1 = ab.select('*', F.row_number().over(windowSpec).alias('row_number')).where(ab['end_date'].isNull())

ab1 = ab1.select(ab.emp_id, ab.designation, ab.start_date, ab.end_date, ab.salary, ab.status).where(ab1.row_number > 1)

ab = ab.exceptAll(ab1)
ab = ab.orderBy("emp_id", "start_date")

#adding columns to be used in task 5
ab=ab.withColumn("strike_count",F.lit(0))
ab=ab.withColumn("updated_salary",ab["salary"])
ab=ab.withColumn("cooldown_start_date",F.lit(None).cast("date"))

ab.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://ec2-3-94-106-128.compute-1.amazonaws.com:5432/postgres") \
    .option("dbtable", "airflow.emp_timeframe_staging") \
    .option("user", "postgres") \
    .option("password", "1234") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

