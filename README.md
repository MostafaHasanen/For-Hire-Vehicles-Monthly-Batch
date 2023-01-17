# For-Hire-Vehicles-Monthly-Batch
with Airflow: Monthly Up to Google Lake raw files Then DataProc uploaded data to BigQuery with SQL requirment as partitioned table on PickupDate
with DBT: Transform the table creating new table for Looker Studio Dashboard (All data is up-to-date in monthly manner 2nd Day of each Month with latest updated data: 3 Month Delay for data availability) e.g: You monitor September Data in December


# Documenation:
Project on Live Batching for NewYork For Hire Vehicles with options for multi-tables for each months or One Single table with all data added when available on the site. with Creation of Dashboard on Looker Studio to monitor the analysis of information of data

Final Files and operations:
    Instructions.md: Monitor of all steps passed by during creation of project, with Errors, Solutions and reasons of updating and specifing Why this solution.
    
    Spark:
        1-Spark-run-ALL.py: Pyspark Code added to Google Cloud Lake to be processed to DataProc Cluster
            File contain SQL and required operations in data reduction and spark configurations to reduce Queries latency
    Airflow: Operations occur with 3 months delay (Time for data availability)
        1-Upload_GCS_Bucket_dag.py: will download and add data to bucket in parquet format in 1st day 6PM of every month
        2-All_BQ_DataProc_dag.py: Append new data to BigQuery table partitioned with Date in 2nd day 6PM of every month

Current Date: 10/1/2023 %D %M %Y : Data till October 2022 (-3 current)
### Note that if you run a DAG on a schedule_interval of one day, the run stamped 2016-01-01 will be trigger soon after 2016-01-01T23:59. In other words, the job instance is started once the period it covers has ended. so in DAG we only minus 2 months in code
    Size of Data: ~728 Million row

    DBT: Required Transformations of data, Adding documentations of process and informations about columns and what they for
        Replace Null and empty values with N(No) or 0
        DBT Runs on new portions of data only (already added partitioned by Date in BQ) 


### Looker Studio Link: https://datastudio.google.com/reporting/e0f12d7e-0aaa-47c1-9087-0e163479fe49
![image](https://user-images.githubusercontent.com/101864501/212838275-71eef67d-392d-4634-a03f-12a012655c63.png)
