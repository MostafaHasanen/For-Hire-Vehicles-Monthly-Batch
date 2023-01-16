from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from datetime import datetime

import os
import logging

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET_NAME = os.environ.get("GCP_GCS_BUCKET")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}
Year_Month = '{{ (execution_date + macros.dateutil.relativedelta.relativedelta(months=-3)).strftime("%Y-%m") }}' 
Year_folder = '{{ (execution_date + macros.dateutil.relativedelta.relativedelta(months=-3)).strftime("%Y") }}'
Output_Location = f"ForHireVehicles.FHVHV_table-{Year_Month}"

PYSPARK_FILE = "code/Spark-run.py"
PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": "spark-sql-cluster"},
    #"jars": ["gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"],
    "pyspark_job": 
    {   "main_python_file_uri": f"gs://{BUCKET_NAME}/{PYSPARK_FILE}",
        "args": [f"--year_month={Year_Month}",f"--year={Year_folder}",f"--output={Output_Location}"],
        #SYNTAX DIFFER FROM DOCUMENT ON GCP
        "jar_file_uris": ["gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"], #https://stackoverflow.com/questions/69775465/cloud-composer-dag-error-java-lang-classnotfoundexception-failed-to-find-dat
    },
}

with DAG(
    "DataProc-Submit-Job-Dag",
    schedule_interval="0 6 2 * *",
    default_args=default_args,
    start_date=datetime(2019, 5, 1),
    catchup=True,
    max_active_runs=1
)as dag:
        dataproc_task = DataprocSubmitJobOperator(
            task_id="dataproc_task", 
            job=PYSPARK_JOB, 
            region="europe-west1", 
            project_id=PROJECT_ID,
            #metadata=[("jars","gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar")],
            #dataproc_spark_jars=["gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"],
        )
        dataproc_task
