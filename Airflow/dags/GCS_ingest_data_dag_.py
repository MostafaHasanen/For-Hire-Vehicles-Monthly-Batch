from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.utils.dates import days_ago

import os
import logging

import pyarrow.csv as pv
import pyarrow.parquet as pq

from google.cloud import storage

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

### Define function to strict default schema "each depends on service type" and rewrite all files schema; since parquet files schemas differ in same service

def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))

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
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 0,
}

def General_ingest_local_to_GCS(
    dag,
    URL_TEMPLATE,
    OUTPUT_FILE_TEMPLATE,
    FILE_TEMPLATE,
    IT_CSV=""
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
                "object_name": f"raw/{FILE_TEMPLATE}",
                "local_file": f"{OUTPUT_FILE_TEMPLATE}",
            },
        )
        # Convert if CSV file to Parquet
        if ".csv" in FILE_TEMPLATE:
            wget_task = BashOperator(
                task_id='wget',
                bash_command=f'curl -sSL {URL_TEMPLATE} > {IT_CSV}',
                do_xcom_push=True
            )
            format_to_parquet_task = PythonOperator(
                task_id="format_to_parquet_task",
                python_callable=format_to_parquet,
                op_kwargs={
                    "src_file": f"{IT_CSV}",
                },
            )
            rm_data = BashOperator(
                task_id='rm',
                bash_command=f'rm {IT_CSV} {OUTPUT_FILE_TEMPLATE}'
            )
            wget_task >> format_to_parquet_task >> local_to_gcs_task >> rm_data
        else:
            wget_task >> local_to_gcs_task >> rm_data

URL_PREFIX = 'https://d37ci6vzurychx.cloudfront.net/trip-data'
# Jinja python in format
URL_TEMPLATE = URL_PREFIX + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
FILE_TEMPLATE = 'output_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME +"/"+ FILE_TEMPLATE

Yellow_Taxi = DAG(
    "Yellow_Taxi_IngestionDag",
    schedule_interval="0 6 1 * *",
    #schedule_interval="@daily"
    # default_args=default_args,
    start_date=datetime(2019, 1, 1),
    end_date = datetime(2021,1,1),
    catchup=True,
    max_active_runs=3
)
General_ingest_local_to_GCS(
    dag=Yellow_Taxi,
    URL_TEMPLATE=URL_TEMPLATE,
    OUTPUT_FILE_TEMPLATE=OUTPUT_FILE_TEMPLATE,
    FILE_TEMPLATE=FILE_TEMPLATE,
)
#URL_PREFIX = 'https://d37ci6vzurychx.cloudfront.net/trip-data'
URL_TEMPLATE = URL_PREFIX + '/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
FILE_TEMPLATE = "green_tripdata/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'

Green_Taxi = DAG(
    dag_id="Green_Taxi_IngestionDag",
    schedule_interval="0 7 2 * *",
    start_date=datetime(2019, 1, 1),
    end_date = datetime(2021,1,1),
    catchup=True,
    max_active_runs=3
)

General_ingest_local_to_GCS(
    dag=Green_Taxi,
    URL_TEMPLATE=URL_TEMPLATE,
    OUTPUT_FILE_TEMPLATE=OUTPUT_FILE_TEMPLATE,
    FILE_TEMPLATE=FILE_TEMPLATE,
)


URL_PREFIX = 'https://d37ci6vzurychx.cloudfront.net/trip-data' 
# Jinja python in format
URL_TEMPLATE = URL_PREFIX + '/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
FILE_TEMPLATE = 'FHV_OUT_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/' + FILE_TEMPLATE

FHVIngestionDag = DAG(
    "FHVIngestionDag",
    schedule_interval="0 6 1 * *",
    #schedule_interval="@daily"
    start_date=datetime(2019, 1, 1),
    end_date = datetime(2020,1,1),
    #catchup=False,
    max_active_runs=3
)
General_ingest_local_to_GCS(
    dag=FHVIngestionDag,
    URL_TEMPLATE=URL_TEMPLATE,
    OUTPUT_FILE_TEMPLATE=OUTPUT_FILE_TEMPLATE,
    FILE_TEMPLATE=FILE_TEMPLATE,
)

URL_TEMPLATE = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv'
FILE_TEMPLATE = 'taxi+_zone_lookup.parquet'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/'+ FILE_TEMPLATE
IT_CSV = AIRFLOW_HOME + '/'+ 'taxi+_zone_lookup.csv'
from airflow.utils.dates import days_ago
ZonesIngestionDag = DAG(
    "ZonesIngestionDag",
    schedule_interval='@once',
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1
)
General_ingest_local_to_GCS(
    dag=ZonesIngestionDag,
    URL_TEMPLATE=URL_TEMPLATE,
    OUTPUT_FILE_TEMPLATE=OUTPUT_FILE_TEMPLATE,
    FILE_TEMPLATE=FILE_TEMPLATE,
    IT_CSV=IT_CSV
)


# Extra dag to sort already added files from /raw to their accurate destination: e.g. Yellow Taxi Data:
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

DATASET = "FHV_OUT" #, "yellow"
INPUT_PART = "raw"
DATASET_NAMING = "trips_data_all"
DATANAMING = "fhv_out" #, "yellow"
with DAG(
    dag_id=f"sort_{DATANAMING}_Taxi",
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
    max_active_runs=1
)as dag:
    move_files_gcs_task = GCSToGCSOperator(
        task_id=f'move_{DATANAMING}_files_task',
        source_bucket=BUCKET,
        source_object=f'{INPUT_PART}/{DATASET}*.parquet',
        destination_bucket=BUCKET,
        destination_object=f'raw/{DATANAMING}/{DATANAMING}',
        move_object=True
    )
    move_files_gcs_task