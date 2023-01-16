# load fhv vehicles data from gcs to bq
import os
import logging

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.utils.dates import days_ago

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

#AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
DATASET_NAMING = "trips_data_all"
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", DATASET_NAMING)

COLOUR_RANGE = {'yellow_tripdata': 'tpep_pickup_datetime', 'green_tripdata': 'lpep_pickup_datetime', 'fhv_out': 'pickup_datetime'}
INPUT_PART = "raw"
INPUT_FILETYPE = "parquet"

Y_COL = f"VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,\
        RatecodeID,store_and_fwd_flag,PULocationID,DOLocationID,payment_type,fare_amount,\
        extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,congestion_surcharge"
        #,CAST(airport_fee as NUMERIC) as airport_fee" ###Error with parquet format (different format for rows)
G_COL =f"VendorID,lpep_pickup_datetime,lpep_dropoff_datetime,store_and_fwd_flag,RatecodeID,\
        PULocationID,DOLocationID,passenger_count,trip_distance,fare_amount,extra,mta_tax,\
        tip_amount,tolls_amount,\
        improvement_surcharge,total_amount,payment_type,trip_type,congestion_surcharge"
        #,CAST(ehail_fee as NUMERIC) as ehail_fee"
F_COL = f"dispatching_base_num,pickup_datetime,dropOff_datetime,Affiliated_base_number"#, PUlocationID,DOlocationID,SR_Flag"
COL_Values = {'yellow_tripdata': Y_COL, 'green_tripdata': G_COL, 'fhv_out': F_COL}
#COL_Values = {'yellow_tripdata': "*", 'green_tripdata': "*", 'fhv_out': "*"}

default_args = {
    "owner": "airflow",
    "start_date": days_ago(0),
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id=f"GCS_2_BQ_TB_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
)as dag:
    for DataSet_Type, ds_col in COLOUR_RANGE.items():    
        gcs_2_bq_task = BigQueryCreateExternalTableOperator(
            task_id=f"bq_{DataSet_Type}_external_table_task",
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": f"{DataSet_Type}_external_table",
                },
                "externalDataConfiguration": {
                    "autodetect": "True",
                    "sourceFormat": f"{INPUT_FILETYPE.upper()}",
                    "sourceUris": [f"gs://{BUCKET}/{INPUT_PART}/{DataSet_Type}/*"],
                    "referenceFileSchemaUri": f"gs://{BUCKET}/{INPUT_PART}/{DataSet_Type}/*2019-01.parquet",
                    "ignoreUnknownValues" : "True",
                },
            },
        )
        ### ERROR FACED: Different Data Types in same column in parquet file (Against its own schema)
        Query_BQ_TB = (
            f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{DataSet_Type} \
            PARTITION BY DATE({ds_col}) \
            AS \
            SELECT {COL_Values[DataSet_Type]} FROM {BIGQUERY_DATASET}.{DataSet_Type}_external_table;"
        )

        # Create a partitioned table from external table
        bq_create_partitioned_table_job = BigQueryInsertJobOperator(
            task_id=f"bq_create_{DataSet_Type}_partitioned_table_task",
            configuration={
                "query": {
                    "query": Query_BQ_TB,
                    "useLegacySql": False,
                }
            }
        )

        gcs_2_bq_task >> bq_create_partitioned_table_job