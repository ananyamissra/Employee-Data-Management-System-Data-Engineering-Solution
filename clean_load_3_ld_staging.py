from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
import boto3
from io import StringIO
#import pandas as pd
from pyspark.sql.functions import col, to_date,monotonically_increasing_id

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

s3_client = boto3.client('s3')

# Read CSV file from S3 directly into a DataFrame
#csv_object = s3_client.get_object(Bucket='ttn-de-bootcamp-2024-bronze-us-east-1',
 #                                 Key='rohan.majhi/Project/Employee_leave_data/employee_leave_data.csv')

# Read leave data from CSV
# Read CSV data into a Pandas DataFrame
#csv_data = csv_object['Body'].read().decode('utf-8')

# Read CSV data into a Pandas DataFrame
#pandas_df = pd.read_csv(StringIO(csv_data))

# Convert Pandas DataFrame to Spark DataFrame
#leave_data_df = spark.createDataFrame(pandas_df)

bucket_name = 'ttn-de-bootcamp-2024-bronze-us-east-1'
folder_prefix = 'rohan.majhi/Project/Employee_leave_data/'

response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)
files = [(obj['Key'], obj['LastModified']) for obj in response.get('Contents', [])]
files.sort(key=lambda x: x[1], reverse=True)
newest_file_key, _ = files[0]


s3_path = f"s3://{bucket_name}/{newest_file_key}"
leave_data_df=spark.read.csv(path=s3_path,header=True,inferSchema=True)

# Adding new column ser_num so to pick latest row in partition of emp_id
leave_data_df = leave_data_df.withColumn("SerialNum", monotonically_increasing_id())

# Define window specification
windowSpec = Window.partitionBy("emp_id", "date").orderBy(col("serialNum").desc())

# Add row numbers to the dataframe, partitioned by employee_id and leave_date
leave_data_with_row_number = leave_data_df.withColumn("row_number", row_number().over(windowSpec))

# Filter out the rows where row_number is 1 (i.e., keep only the first row in each group)
deduplicated_leave_data_df = leave_data_with_row_number.filter(col("row_number") == 1)

# Show the resulting dataframe
deduplicated_leave_data_df = deduplicated_leave_data_df.drop("row_number").drop("SerialNum")
deduplicated_leave_data_df = deduplicated_leave_data_df.withColumn("date", to_date(col('date'), 'yyyy-MM-dd'))
# df = df.withColumn('date', to_date(col('date'), 'yyyy-MM-dd'))
deduplicated_leave_data_df.printSchema()
deduplicated_leave_data_df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://ec2-3-94-106-128.compute-1.amazonaws.com:5432/postgres") \
    .option("dbtable", "airflow.leave_data_staging") \
    .option("user", "postgres") \
    .option("password", "1234") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

