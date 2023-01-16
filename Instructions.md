We Working with High Volume For-Hire Vehicle Trip Records
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

download data where Spark working (GCP Virtual Machine for me) and lookup using tree command

open notebook take a snippit of the data and see what needs to be done

Add your concluded SQL code on file then process to bigquery in airflow

transition now to DBT to work
Union with the lookuptable create table for LookerStudio

### First Option
# If data is saved on GCP Lake
# Dataproc on GCP for running spark code with saved data on gcp
upload script to lake
gsutil cp /home/7asanen999/NY_Taxi_monthly_Spark_Batchs/Spark/Spark-run-ALL.py gs://dtc_data_lake_de-zoom-359609/code/Spark-run-ALL.py
# Run your uploaded script
# year_month=execution_date.strftime("%Y-%m") (in Airflow)
gcloud dataproc clusters start spark-sql-cluster \
    --region=europe-west1

gcloud dataproc jobs submit pyspark \
    --cluster=spark-sql-cluster \
    --region=europe-west1 \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    gs://dtc_data_lake_de-zoom-359609/code/Spark-run.py \
    -- \
        --year_month=2019-02 \
        --year=2019 \
        --output=ForHireVehicles.FHVHV_table-2019-02
# run this for 19 20 21 22 (Can we schedule it!) to run monthly! ### DONE!!! ###
options in saving parquets tables into BigQuery
    .partitionBy("date") # allow tables to be unique in big table for faster queries
    .clusterBy(50, "id") # ease in search
    .mode("append") # To have all data in single table added monthly
SQL: SELECT count(*) FROM `de-zoom-359609.ForHireVehicles.FHVHV_table-*` # with multi additions to try on BQ

### Second Option
# Processing data month by month without saving raw files only processed on BigQuery
Start Spark cluster locally "stand-alone":https://spark.apache.org/docs/latest/spark-standalone.html
Master:
/home/7asanen999/spark/spark-3.3.1-bin-hadoop3/sbin/start-master.sh --webui-port 4040 ::: Starts on port 

Creating a worker:
URL="spark://de-zoomcamp.europe-west1-b.c.de-zoom-359609.internal:7077"
/home/7asanen999/spark/spark-3.3.1-bin-hadoop3/sbin/start-worker.sh ${URL}

Spark Connection — Create Spark connection in Airflow web ui :
    (localhost:8080) > admin menu > connections > add+ > Choose Spark as the connection type, 
    give a connection id (spark_standalone) and put the Spark master url (i.e local[*], or the cluster manager master’s URL) 
    and also port of your Spark master or cluster manager if you have an Spark cluster
Set Spark app home variable "This is very useful to define a global variable in Airflow to be used in any DAG. 
I define the PySpark app home dir as an Airflow variable which will be used later. In admin menu, hit the variable and define the variable


Convert notebook to script:
jupyter nbconvert --to=script 2_SQL_Spark.ipynb

Use `spark-submit` for running the script on the cluster
# We need to run next job but in Dag after download before rm
spark-submit \
    --master="${URL}" \
    /home/7asanen999/NY_Taxi_monthly_Spark_Batchs/Airflow/Codes/Spark-run_2ndApproach_moreSQL.py \
        --FILE_TEMPLATE=/opt/***/fhvhv_2019-02.parquet

spark-submit 
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,io.delta:delta-core_2.12:0.7.0 
    --master local[*] 
    --driver-memory 12g 
    --executor-memory 12g 
    spark/search_event_ingestor.py


### Side issues: Linux Storage eaten up with tempfs (command: df -h) ::: 
# sol: 
    X 1- docker volume rm $(docker volume ls -q)
    2-  # danger, read the entire text around this code before running
        # you will lose data
        sudo -s
        systemctl stop docker
        rm -rf /var/lib/docker
        systemctl start docker
        exit

no cache run docker:  docker-compose up --build -d

# Copy from gs the jar files into ur Airflow to send to spark: gsutil cp gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar spark-bigquery-latest_2.12.jar
# org.apache.hadoop.fs.UnsupportedFileSystemException: No FileSystem for scheme "gs": gsutil cp gs://hadoop-lib/gcs/gcs-connector-hadoop3-2.2.5.jar gcs-connector-hadoop3-2.2.5.jar
# gsutil cp gs://hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar gcs-connector-hadoop3-latest.jar
# curl https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop2-latest.jar > gcs-connector-hadoop2-latest.jar

### Hadoop error consistant: Recalcualte billing and most comments recommended sticking to Dataproc: as data already uploaded in Parquet format: processing to BQ in same region will NOT add Costs


### DBT section:
# Could use dbt core on device with Orchisteration of BashOperators to run the jobs with execution date in partitioned manner / Connect to dbt cloud if in organization account
# in CI drop the existed BigQuery then run the DBT job (resource efficiency)
    Add lookuptable, map credit options, process SQL if needed: Save table for Looker:
    Replace Null and empty values with N(No) or 0
    DBT Runs on new portions of data only (already added partitioned by Date in BQ) 




///Future tips
    -- materialized='incremental',
    -- partition_by={'field': 'pickup_datetime', 'data_type': 'date'},
    -- incremental_strategy='insert_overwrite',
    -- partitions=[formatted_date(var('execution_date'))]

