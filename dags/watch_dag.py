from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import asyncio
import json
import os

from extract import extract_links, extract_watch_data
from transform import transform_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    'etl_watch_data_pipeline',
    default_args=default_args,
    description='ETL Pipeline for Watch Data',
    schedule_interval=None,  
    start_date=days_ago(1),
    catchup=False,
) as dag:

    extracted_data_path = 'data/extracted_data.json'
    transformed_data_path = 'data/transformed_data.csv'
    s3_bucket = 'watchesdata'
    s3_file_key = 'watches/transformed_data.csv'

    def check_file_content(filepath, description):
        if not os.path.exists(filepath):
            return False
        
        file_size = os.path.getsize(filepath)
        if file_size == 0:
            return False
            
        return True

    def extract_task():
        try:
            
            extracted_links = asyncio.run(extract_links())
            if not extracted_links:
                raise ValueError("No links were extracted")
            
            
            watch_data = asyncio.run(extract_watch_data(extracted_links))
            if not watch_data:
                raise ValueError("No watch data was extracted")
            
            
            with open(extracted_data_path, 'w') as f:
                json.dump(watch_data, f)
            
            if not check_file_content(extracted_data_path, "Extracted data"):
                raise ValueError("Failed to save extracted data properly")                
            
        except Exception as e:
            raise
    def transform_task():
        try:
            
            if not check_file_content(extracted_data_path, "Input"):
                raise ValueError(f"Input file {extracted_data_path} is missing or empty")

            with open(extracted_data_path, 'r') as f:
                data = json.load(f)
            
            transformed_data = transform_data(data)
            if not transformed_data:
                raise ValueError("Transform function returned empty data")
            
            with open(transformed_data_path, 'w') as f:
                f.write(transformed_data)
            
            if not check_file_content(transformed_data_path, "Transformed data"):
                raise ValueError("Failed to save transformed data properly")
                            
        except Exception as e:
            raise
        
    def load_task():
        try:
            
            if not check_file_content(transformed_data_path, "Input"):
                raise ValueError(f"Input file {transformed_data_path} is missing or empty")
            
            s3_hook = S3Hook(aws_conn_id='aws_default')
            
            s3_hook.load_file(
                filename=transformed_data_path,
                key=s3_file_key,
                bucket_name=s3_bucket,
                replace=True
            )
            
            if not s3_hook.check_for_key(s3_file_key, bucket_name=s3_bucket):
                raise ValueError(f"File was not found in S3 after upload")
            
            s3_file_size = s3_hook.get_key(s3_file_key, bucket_name=s3_bucket).content_length
            
            if s3_file_size == 0:
                raise ValueError("Uploaded file is empty in S3")
            
        except Exception as e:
            raise

    extract = PythonOperator(
        task_id='extract',
        python_callable=extract_task
    )

    transform = PythonOperator(
        task_id='transform',
        python_callable=transform_task
    )

    load = PythonOperator(
        task_id='load',
        python_callable=load_task
    )

    extract >> transform >> load