from datetime import datetime,timedelta,time
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.empty import EmptyOperator
import pandas as pd
from airflow.operators.python import ShortCircuitOperator
import os

default_arguments = {
    'owner': 'ankaro',
    'retries': 3,
    'retry_delay':timedelta(minutes=1)
}

with DAG(
        dag_id='project_dag_daily_final_1',
        start_date=datetime(2024, 5, 20),
        default_args=default_arguments,
        schedule_interval='0 7 * * *',
        catchup=False
) as dag:
    start = EmptyOperator(task_id='start')

    def check_new_file(folder_prefix):
            import pytz
            import boto3

            s3_client = boto3.client('s3')
            bucket_name = 'ttn-de-bootcamp-2024-bronze-us-east-1'
            #folder_prefix = 'rohan.majhi/Project/Employee_data/'

            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)

            if 'Contents' not in response:
                return False

            files = [(obj['Key'], obj['LastModified']) for obj in response.get('Contents', [])]
    
            # Sort files by last modified timestamp (newest first)
            files.sort(key=lambda x: x[1], reverse=True)

            # Get the newest file's last modified timestamp
            newest_file_key, newest_file_timestamp = files[0]

            # Define the UTC timezone
            utc_timezone = pytz.UTC

            # Define the time window or condition
            start_time_naive = datetime.combine(datetime.now(), time(0, 0))  # 12 AM today (timezone-naive)
            end_time_naive = datetime.combine(datetime.now(), time(7, 0))  # 7 AM today (timezone-naive)

            # Convert time window to timezone-aware datetimes
            start_time = utc_timezone.localize(start_time_naive)
            end_time = utc_timezone.localize(end_time_naive)

            # Check if the newest file is within the time window
            if start_time <= newest_file_timestamp <= end_time:
                return True
            else:
                return False
    

    def add_emr_step(s3_file_location):
        import boto3
        from time import sleep
        client = boto3.client('emr', region_name='us-east-1')
        cluster_id = 'j-2UVWGTM2B1N1E'
        response = client.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[
                {
                    'Name': 'Spark application',
                    'ActionOnFailure': 'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': [
                            'spark-submit',
                            '--deploy-mode', 'client',
                            '--master', 'yarn',
                            '--executor-memory', '2G',
                            '--executor-cores', '1',
                            '--num-executors', '2',
                            '--jars', '/usr/lib/spark/jars/postgresql-42.7.3.jar',
                            s3_file_location
                        ]
                    }
                }
            ]
        )

        # Extract the step ID
        step_id = response['StepIds'][0]

        # Function to get the status of the step
        def get_step_status(client, cluster_id, step_id):
            response = client.describe_step(
                ClusterId=cluster_id,
                StepId=step_id
            )
            return response['Step']['Status']['State']

        # Loop to get the status at every 30seconds
        while True:
            status = get_step_status(client, cluster_id, step_id)
            if status in ['COMPLETED', 'FAILED', 'CANCELLED']:
                break
            print(f"Step {step_id} is still {status}. Waiting...")
            sleep(30)  # Wait for 30 seconds before checking the status again

        # Check final status
        if status == 'COMPLETED':
            print(f"Step {step_id} completed successfully.")
        else:
            print(f"Step {step_id} failed with status: {status}")

        return status

    def load1():
        from pyspark.sql import SparkSession
        import boto3

        s3_client = boto3.client('s3')

        bucket_name = 'ttn-de-bootcamp-2024-bronze-us-east-1'
        folder_prefix = 'rohan.majhi/Project/Employee_data/'

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

        table_name = 'airflow.emp_data_staging'

        pg_hook.insert_rows(table=table_name, rows=df.values.tolist())

    ####################################
    #Task 1, checks if new file is there

    check_new_file_ed=ShortCircuitOperator(task_id="check_new_file_ed",python_callable=check_new_file,op_args=["rohan.majhi/Project/Employee_data/"])

    #Loads the new file into staging table 

    load_1_ed_staging=PythonOperator(task_id="1_loading_emp_staging",python_callable=load1)

    # Merges staging with actual data(append only)
    merge1_ed_dim_staging=PostgresOperator(task_id='1_merge_table_emp_data_staging',
                              sql="""
                                INSERT INTO airflow.emp_data_dim (emp_id, emp_name, emp_age)
                                SELECT source.emp_id, source.emp_name, source.emp_age
                                FROM airflow.emp_data_staging AS source;
                                truncate table airflow.emp_data_staging
                                 """,
                              postgres_conn_id='postgres_conn')

    ###################################
    #Task 2, check if new file has came
    check_new_file_tf=ShortCircuitOperator(task_id="check_new_file_tf",python_callable=check_new_file,op_args=["rohan.majhi/Project/Employee_timeframe_data/"])

    #Cleaning of new file and loading into staging table of timeframe through EMR
    clean_load_2_tf_staging = PythonOperator(
        task_id='add_emr_clean_load_2_tf_staging',
        python_callable=add_emr_step,
        op_args=["s3://ttn-de-bootcamp-2024-bronze-us-east-1/rohan.majhi/spark_code_python/clean_load_2_tf_staging.py"]
    )
    
    #Merging staging table with actual table through EMR (UpSert)
    merge2_tf_dim_staging = PythonOperator(
        task_id='add_emr_merge2_tf_dim_staging',
        python_callable=add_emr_step,
        op_args=["s3://ttn-de-bootcamp-2024-bronze-us-east-1/rohan.majhi/spark_code_python/merge2_tf_dim_staging.py"]
    )
    
    ##############################
    #Task 3, Check if new has came 
    check_new_file_ld=ShortCircuitOperator(task_id="check_new_file_ld",python_callable=check_new_file,op_args=["rohan.majhi/Project/Employee_leave_data/"])

    #Cleans the new file and loads into staging table of leave data through EMR 
    clean_load_3_ld_staging = PythonOperator(
        task_id='add_emr_clean_load_3_ld_staging',
        python_callable=add_emr_step,
        op_args=["s3://ttn-de-bootcamp-2024-bronze-us-east-1/rohan.majhi/spark_code_python/clean_load_3_ld_staging.py"]
    )

    #Merges the staging table with actual data (UpSert)
    merge3_ld_dim_staging = PostgresOperator(task_id='merge_table_leave_data_staging',
                              sql="""
                                    MERGE INTO airflow.leave_data_dim AS h
                                    USING airflow.leave_data_staging AS n
                                    ON h.emp_id = n.emp_id AND h.date = n.date
                                    WHEN MATCHED THEN UPDATE 
                                        SET status = n.status
                                    WHEN NOT MATCHED THEN 
                                        INSERT (emp_id, date, status)
                                        VALUES (n.emp_id, n.date, n.status);
                                        truncate table airflow.leave_data_staging;
                                         """,
                              postgres_conn_id='postgres_conn')


    
    ################################################
    #Generating active employees file on daily basis through EMR
    #Writing it as csv in s3 and as table in DB

    generate_active_emp_csv = PythonOperator(
        task_id='add_emr_active_emp_csv',
        python_callable=add_emr_step,
        op_args=["s3://ttn-de-bootcamp-2024-bronze-us-east-1/rohan.majhi/spark_code_python/Task_4_generate_active_table.py"]
    )
    
    #############################
    #Generating employees with more than 8 percent leave applied through EMR
    #Writing it as csv in s3 and as table in DB
    generate_8_percent_emp_table_csv = PythonOperator(
        task_id='add_emr_8_percent_emp_table_csv',
        python_callable=add_emr_step,
        op_args=["s3://ttn-de-bootcamp-2024-bronze-us-east-1/rohan.majhi/spark_code_python/Task4_8percent_emp.py"]
    )


    #Task1 emp_data
    start>>check_new_file_ed>>load_1_ed_staging>>merge1_ed_dim_staging

    #Task 2 emp_timeframe using pyspark
    start>>check_new_file_tf>>clean_load_2_tf_staging>>merge2_tf_dim_staging
 
    #Task3 leave_data loading using pyspark and psql
    start>>check_new_file_ld>>clean_load_3_ld_staging>>merge3_ld_dim_staging

    #Task4 Active emp table creation, 8% Percentage employees
    start>>generate_active_emp_csv>>generate_8_percent_emp_table_csv
