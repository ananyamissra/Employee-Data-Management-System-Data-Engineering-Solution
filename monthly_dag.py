from datetime import datetime,timedelta,time

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.empty import EmptyOperator
import pandas as pd
#from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from io import StringIO
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os

default_arguments = {
    'owner': 'ankaro',
    'retries': 3,
    'retry_delay':timedelta(minutes=1)
}

with DAG(
        dag_id='project_dag_monthly_final',
        start_date=datetime(2024, 3, 14),
        default_args=default_arguments,
        schedule_interval='0 7 * * *',
        catchup=False
) as dag:
    start = EmptyOperator(task_id='start')
    

    def add_emr_step(s3_file_location):
        import boto3
        from time import sleep
        client = boto3.client('emr', region_name='us-east-1')  # Ensure your region is correct
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

        # Poll the step status
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
    

    generate_80_percent_emp_table_csv = PythonOperator(
        task_id='80_percent_emp',
        python_callable=add_emr_step,
        op_args=["s3://ttn-de-bootcamp-2024-bronze-us-east-1/rohan.majhi/spark_code_python/Task_4_monthly_80_percent.py"]
    )