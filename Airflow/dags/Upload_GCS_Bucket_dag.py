from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

import os
import logging

from google.cloud import storage

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

def upload_to_gcs(**kwargs):
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround
    client = storage.Client()
    bucket = client.bucket(kwargs["bucket"])
    blob = bucket.blob(kwargs["object_name"])
    blob.upload_from_filename(kwargs["local_file"])

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}

def General_ingest_local_to_GCS(
    dag,
    URL_TEMPLATE,
    OUTPUT_FILE_TEMPLATE,
    FILE_TEMPLATE,
    YEAR
):
    with dag:
        wget_task = BashOperator(
            task_id='wget',
            #To make it fail on 404, add the `-sSLf` flag: data not exist
            ### stream the output of curl to GCS
            bash_command=f'curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}',
            do_xcom_push=True
        )
        rm_data = BashOperator(
            task_id='rm',
            bash_command=f'rm {OUTPUT_FILE_TEMPLATE}'
        )
        local_to_gcs_task = PythonOperator(
            task_id="local_to_gcs_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": f"raw/{YEAR}/{FILE_TEMPLATE}", # Change raw folder for future work
                "local_file": f"{OUTPUT_FILE_TEMPLATE}",
            },
        )
        wget_task >> local_to_gcs_task >> rm_data

URL_PREFIX = 'https://d37ci6vzurychx.cloudfront.net/trip-data'
# Jinja python in format: subtract 3 months from format: for data availability
Month_Year = '{{ (execution_date + macros.dateutil.relativedelta.relativedelta(months=-2)).strftime("%Y-%m") }}' 

URL_TEMPLATE = URL_PREFIX + f'/fhvhv_tripdata_{Month_Year}.parquet'
FILE_TEMPLATE = f'fhvhv_{Month_Year}.parquet'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME +"/"+ FILE_TEMPLATE

Year_folder = '{{ (execution_date + macros.dateutil.relativedelta.relativedelta(months=-2)).strftime("%Y") }}'

FHVHV_data = DAG(
    "For-Hire-Vehicles-Dag",
    schedule_interval="0 6 1 * *",
    # default_args=default_args,
    start_date=datetime(2019, 4, 1),
    catchup=True,
    max_active_runs=1
)
General_ingest_local_to_GCS(
    dag=FHVHV_data,
    URL_TEMPLATE=URL_TEMPLATE,
    OUTPUT_FILE_TEMPLATE=OUTPUT_FILE_TEMPLATE,
    FILE_TEMPLATE=FILE_TEMPLATE,
    YEAR=Year_folder
)