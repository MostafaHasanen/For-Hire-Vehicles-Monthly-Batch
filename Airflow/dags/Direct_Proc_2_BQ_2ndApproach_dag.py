from airflow import DAG
from datetime import datetime

from airflow.operators.bash import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import Variable

import os
import logging

from google.cloud import storage

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
#AIRFLOW_STAGING = "/home/7asanen999/NY_Taxi_monthly_Spark_Batchs/Airflow/Staging"

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}

def Ingest_Local_2_BQ(
    dag,
    URL_TEMPLATE,
    OUTPUT_FILE_TEMPLATE,
):
    with dag:
        wget_task = BashOperator(
            task_id='wget',
            #To make it fail on 404, add the `-sSLf` flag: data not exist
            bash_command=f'curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}',
            do_xcom_push=True
        )
        rm_data = BashOperator(
            task_id='rm',
            bash_command=f'rm {OUTPUT_FILE_TEMPLATE}'
        )
        # Work to upload with spark into BQ directly
        submit_job_spark_BQ = SparkSubmitOperator(
            task_id='BQ-All-Table',
            conn_id='spark_standalone',
            application=f'./codes/Spark-run_2ndApproach_moreSQL.py',
            application_args=[f"--FILE_TEMPLATE={OUTPUT_FILE_TEMPLATE}"],
            jars="./codes/lib/spark-bigquery-latest_2.12.jar"
        )
        wget_task >> submit_job_spark_BQ >> rm_data

URL_PREFIX = 'https://d37ci6vzurychx.cloudfront.net/trip-data'
# Jinja python in format: subtract 3 months from format: for data availability
Month_Year = '{{ (logical_date + macros.dateutil.relativedelta.relativedelta(months=-3)).strftime("%Y-%m") }}' 
Year_folder = '{{ (logical_date + macros.dateutil.relativedelta.relativedelta(months=-3)).strftime("%Y") }}'

URL_TEMPLATE = URL_PREFIX + f'/fhvhv_tripdata_{Month_Year}.parquet'
FILE_TEMPLATE = f'fhvhv_{Month_Year}.parquet'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME +"/"+ FILE_TEMPLATE

FHVHV_data = DAG(
    "For-Hire-Vehicles-Direct-BQ-Dag",
    schedule_interval="0 6 1 * *",
    default_args=default_args,
    start_date=datetime(2019, 5, 1),
    catchup=True,
    max_active_runs=1
)
Ingest_Local_2_BQ(
    dag=FHVHV_data,
    URL_TEMPLATE=URL_TEMPLATE,
    OUTPUT_FILE_TEMPLATE=OUTPUT_FILE_TEMPLATE,
)