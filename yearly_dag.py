from datetime import datetime,timedelta

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.empty import EmptyOperator
import pandas as pd
#from io import StringIO
from airflow.providers.postgres.hooks.postgres import PostgresHook
#from airflow.operators.python import ShortCircuitOperator
import os

default_arguments = {
    'owner': 'ankaro',
    'retries': 2,
    'retry_delay':timedelta(minutes=10)

}

with DAG(
        dag_id='project_dag_yearly_final',
        start_date=datetime(2024, 5, 21),
        default_args=default_arguments,
        schedule='@yearly',
        catchup=False
) as dag:
    start = EmptyOperator(task_id='start')
    


    def load1(table_name,folder_prefix):
        from pyspark.sql import SparkSession
        import boto3


        s3_client = boto3.client('s3')


        bucket_name = 'ttn-de-bootcamp-2024-bronze-us-east-1'
        # folder_prefix = 'rohan.majhi/Project/leave_calendar/'

        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)

        files = [(obj['Key'], obj['LastModified']) for obj in response.get('Contents', [])]

        # Sort files by last modified timestamp (newest first)
        files.sort(key=lambda x: x[1], reverse=True)

        # Get the newest file
        newest_file_key, _ = files[0]

        # Read the contents of the newest file
        obj = s3_client.get_object(Bucket=bucket_name, Key=newest_file_key)

        df=pd.read_csv(obj['Body'])

        pg_hook = PostgresHook(postgres_conn_id='postgres_conn')

        #table_name = 'airflow.emp_data_staging'

        pg_hook.insert_rows(table=table_name, rows=df.values.tolist())

    # def load1():
    #     from pyspark.sql import SparkSession
    #     import boto3

    #     table_name = "airflow.leave_quota_staging"

    #     s3_client = boto3.client('s3')

        
    #     csv_object = s3_client.get_object(Bucket='ttn-de-bootcamp-2024-bronze-us-east-1',
    #                                       Key='rohan.majhi/Project/Employee_leave_quota/employee_leave_quota_data.csv')
        
    #     df=pd.read_csv(csv_object['Body'])

    #     pg_hook = PostgresHook(postgres_conn_id='postgres_conn')

    #     pg_hook.insert_rows(table=table_name, rows=df.values.tolist())

    load=PythonOperator(task_id="load1_into_leave_quota_staging",python_callable=load1,\
                         op_args=['airflow.leave_quota_staging','rohan.majhi/Project/Employee_leave_calendar/'])

    merge1=PostgresOperator(task_id='merge_table_leave_quota_dim_staging',
                              sql="""
                                INSERT INTO airflow.leave_quota_dim (emp_id, leave_quota, leave_year)
                                SELECT source.emp_id, source.leave_quota, source.leave_year
                                FROM airflow.leave_quota_staging AS source;
                                truncate table airflow.leave_quota_staging
                                 """,
                              postgres_conn_id='postgres_conn')

    # def load2():
    #     from pyspark.sql import SparkSession
    #     import boto3

    #     table_name = "airflow.leave_calendar_staging"

    #     s3_client = boto3.client('s3')

    #     # Read CSV file from S3 directly into a DataFrame
    #     csv_object = s3_client.get_object(Bucket='ttn-de-bootcamp-2024-bronze-us-east-1',
    #                                       Key='rohan.majhi/Project/Employee_leave_calendar/employee_leave_calendar_data.csv')
    #     df=pd.read_csv(csv_object['Body'])
    #     pg_hook = PostgresHook(postgres_conn_id='postgres_conn')

    #     # table_name = 'airflow.emp_leave_calendar_dim'

    #     # schema_name='airflow'

    #     pg_hook.insert_rows(table=table_name, rows=df.values.tolist())

    load2=PythonOperator(task_id="load2_into_leave_calendar_staging",python_callable=load1,\
                         op_args=['airflow.leave_calendar_staging','rohan.majhi/Project/Employee_leave_calendar/'])
    merge2=PostgresOperator(task_id='merge_table_leave_calendar_dim_staging',
                              sql="""
                                INSERT INTO airflow.leave_calendar_dim (reason, date)
                                SELECT source.reason, source.date
                                FROM airflow.leave_calendar_staging AS source;
                                truncate table airflow.leave_calendar_staging
                                 """,
                              postgres_conn_id='postgres_conn')

    start>>load>>merge1
    start>>load2>>merge2
