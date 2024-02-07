from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.sensors.http_sensor import HttpSensor
from datetime import datetime, timedelta, date
from functions.helper_function import *
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from airflow.providers.http.operators.http import SimpleHttpOperator
import logging
from azure.storage.blob import BlobServiceClient, ContentSettings
import os

end_point = Variable.get('end_point', default_var=None)
postgres_connection = Variable.get('postgres_conn', default_var=None)
discord_connection = Variable.get('discord_conn', default_var=None)

AZURE_CONNECTION_STRING = Variable.get("azure_conn", default_var=None)
BLOB_CONTAINER_NAME = "cihan"
local_dir = "dags/outputs"


def on_failure_callback(context):
    error_message = f"Airflow pipeline has encountered an issue at {datetime.now()}!"
    logging.error(error_message)
    
    # Add the Discord notification
    discord_webhook_task = SimpleHttpOperator(
        task_id='discord_notification_failure',
        http_conn_id='discord_webhook',
        endpoint=end_point,
        method='POST',
        data='{"content": "' + error_message + '"}',
        headers={"Content-Type": "application/json"},
    )
    discord_webhook_task.execute(context=context)

def on_success_callback(context):
    success_message = f"Airflow pipeline has completed successfully at {datetime.now()}!"
    logging.info(success_message)
    
    # Add the Discord notification
    discord_webhook_task = SimpleHttpOperator(
        task_id='discord_notification_success',
        http_conn_id='discord_webhook',
        endpoint=end_point,
        method='POST',
        data='{"content": "' + success_message + '"}',
        headers={"Content-Type": "application/json"},
    )
    discord_webhook_task.execute(context=context)

def upload_to_azure_blob(local_dir, blob_name):
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(BLOB_CONTAINER_NAME)

    for filename in os.listdir(local_dir):
        if filename.endswith('.csv'):
            blob_path = blob_name + '/' + filename
            blob_client = container_client.get_blob_client(blob_path)

            try:
                with open(os.path.join(local_dir, filename), "rb") as data:
                    blob_client.upload_blob(data, overwrite=True, content_settings=ContentSettings(content_type='text/csv'))
                print(f"Uploaded {filename} to Azure Blob Storage: {blob_path}")
            except Exception as e:
                print(f"Failed to upload {filename} to Azure Blob Storage. Error: {str(e)}")

default_args = {
    'retries': 0,
    'retry_delay': timedelta(minutes=60)
}

today = datetime.today

with DAG(
    default_args=default_args,
    start_date=datetime(2024, 2, 1),
    catchup=False,
    dag_id="Izmir_Market_Price",
    schedule_interval= '@daily',
    on_failure_callback=on_failure_callback,
    on_success_callback=on_success_callback,
) as dag:
    
    start_task = DummyOperator(
        task_id='start_task'
    )

    generate_url_task = PythonOperator(
        task_id='generate_url_task',
        python_callable=generate_url,
        dag=dag,
        op_kwargs={
            'date':datetime.today().date()
        },
        provide_context=True
    )

    fetch_data_task = PythonOperator(
        task_id='fetch_data_task',
        python_callable=fetch_data,
        dag=dag,
        provide_context=True,
    )

    json_to_dataframe_task = PythonOperator(
        task_id='json_to_dataframe_task',
        python_callable=json_to_dataframe,
        dag=dag,
        provide_context=True,
    )

    dataframe_to_csv_task = PythonOperator(
        task_id='dataframe_to_csv_task',
        python_callable=dataframe_to_csv,
        dag=dag,
        provide_context=True,
    )

    upload_csv_to_blob_task = PythonOperator(
        task_id='upload_csv_to_blob',
        python_callable=upload_to_azure_blob,
        op_kwargs={'local_dir': local_dir, 'blob_name': 'Izmir_Market_Price'},
        dag=dag,
    )

    upload_postgres_task = PythonOperator(
        task_id='upload_postgres_task',
        python_callable=upload_postgres,
        dag=dag,
        provide_context=True,
        op_kwargs={
            'csv_file_path':f'dags/outputs/Izmir_Market_Price_{datetime.today().date()}'
        }
    )

    start_task >> generate_url_task >> fetch_data_task >> json_to_dataframe_task >> dataframe_to_csv_task
    dataframe_to_csv_task >> upload_csv_to_blob_task >> upload_postgres_task