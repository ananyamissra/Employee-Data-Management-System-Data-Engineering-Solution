from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *
from pyspark.sql.types import LongType, DateType
from datetime import datetime, timezone
import sys
import json

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
    # jdbc_url = sys.argv[3]
    # connection_properties = sys.argv[4]
    #
    # connection_properties = json.loads(connection_properties)

    # Read data
calendar_data = spark.read.jdbc(url=jdbc_url,  table='airflow.leave_calendar_dim', properties=properties)
leave_data = spark.read.jdbc(surl=jdbc_url,  table='airflow.leave_data_dim', properties=properties)

# Get current year and current date
current_year = datetime.now().year
current_date = datetime.now().date()

#Filtering the data for current_year
calendar_data = calendar_data.filter(year("date")==current_year)
leave_data = leave_data.filter(year("date")==current_year)

# Convert date columns to DateType
to_date = lambda col: col.cast(DateType())
leave_data = leave_data.withColumn("leave_date", to_date(col("date"))).drop("date")
calendar_data = calendar_data.withColumn("calendar_date", to_date(col("date"))).drop("date")

# Filter leave data of leaves which are about to come
leave_data_filtered = leave_data.filter(col("leave_date") > current_date)

# Filter the DataFrame to include only dates after the current date
holidays_upcoming = calendar_data.filter(col("calendar_date") > current_date)
holidays_upcoming = holidays_upcoming.filter(dayofweek(col("calendar_date")).isin([2, 3, 4, 5, 6]))

# Count the rows in the filtered DataFrame
holiday_days_count = holidays_upcoming.count()

# Collect calendar_date values into a list
calendar_dates = [row.calendar_date for row in holidays_upcoming.select("calendar_date").collect()]

# Define expression for weekends 1 is Sunday and 7 is Saturday
weekend_expr = dayofweek(col("leave_date")).isin([1, 7])

# Filter out weekends and holidays
leave_data_filtered = leave_data_filtered.filter(~(weekend_expr | col("leave_date").isin(calendar_dates)))
leave_data_filtered = leave_data_filtered.filter(col("status") != "CANCELLED")

# Remove duplicates
leave_data_filtered = leave_data_filtered.dropDuplicates(["emp_id", "leave_date"])

# Calculate total leaves taken by each employee
leave_count = leave_data_filtered.groupBy("emp_id").agg(count("*").alias("upcoming_leaves"))

# Calculate end_date for the current year
end_date = "{}-12-31".format(current_year)

# Calculate the number of days between current date and end date
days_diff = spark.sql(f"SELECT datediff(to_date('{end_date}'), current_date()) + 1 as days_diff").collect()[0]['days_diff']

# Create date range
date_range = spark.range(0, days_diff).select(expr("date_add(current_date(), cast(id as int))").alias("date"))

# Filter out weekends (Saturday and Sunday)
working_days = date_range.filter(dayofweek("date").isin([2, 3, 4, 5, 6])).count()
total_working_days = working_days - holiday_days_count

# Calculate potential leaves for the current year
potential_leaves_year = leave_count.withColumn("potential_leaves_percentage", (col("upcoming_leaves") / total_working_days) * 100)
upcoming_leaves = potential_leaves_year.select("emp_id","upcoming_leaves").filter(col("potential_leaves_percentage") > 8)
#potential_leaves_year.orderBy("emp_id").show(30)
#upcoming_leaves.orderBy("emp_id").show(30)

upcoming_leaves.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://ec2-3-94-106-128.compute-1.amazonaws.com:5432/postgres") \
    .option("dbtable", "airflow.emp_upcoming_8_fact") \
    .option("user", "postgres") \
    .option("password", "1234") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()