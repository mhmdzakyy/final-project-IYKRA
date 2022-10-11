##################################
import pandas as pd
import numpy as np
from datetime import datetime
from datetime import date
from datetime import timedelta

import os
import logging

import warnings
from glob import glob
from typing import Optional, Sequence, Union
from airflow.models import BaseOperator
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.decorators import apply_defaults

# from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
# from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
# from airflow.contrib.operators import GoogleCloudStorageToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
# from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
###################################
# V A R I A B L E S
today = date.today()
yesterday = today - timedelta(days = 1)

BUCKET_NAME = "fellowship7-finalproject"
FOLDER_NAME = "FINALPROJECT"
# CSV_NAME = "bank_{}".format(yesterday)
CSV_NAME = "bank_2022-10-01"

PROJECT_ID = "data-fellowship7"
DATASET_NAME = "final_project"

# LOCAL_FILE_PATH = "/.google/credentials/airtravel.csv"
# LOCAL_FILE_PATH = "/IYKRA/Final_Project_IYKRA/dataset/bank_2022-10-09.csv"
# LOCAL_FILE_PATH = "/.google/credentials/bank.csv"
LOCAL_FILE_PATH = "/.google/credentials/{}.csv".format(CSV_NAME)
DESTINATION_FILE_LOCATION_URI = "gs://{}/{}".format(BUCKET_NAME,FOLDER_NAME)

#Default args

default_args = {
    "owner": "airflow",
    "start_date": datetime(2022, 10, 1),
    "depends_on_past": False,
    "retries": 1
}

######## DAG
dag = DAG("local_gcs_bq_datamart",
          schedule_interval="0 1 * * *",
          default_args=default_args,
          catchup=False)

#Function 1

# def format_to_parquet(src_file):
#     if not src_file.endswith('.csv'):
#         logging.error("Can only accept source files in CSV format, for the moment")
#         return
#     table = pv.read_csv(src_file)
#     pq.write_table(table, src_file.replace('.csv', '.parquet'))

#Function 2

def uploadtoGCS(csv_name: str, folder_name: str,
                   bucket_name= BUCKET_NAME, **kwargs):

    hook = GoogleCloudStorageHook()
    hook.upload(bucket_name, 
                object_name="{}/{}.csv".format(folder_name, csv_name), 
                filename= LOCAL_FILE_PATH, 
                mime_type='text/csv')

uploadtoGCS_task = PythonOperator(
    task_id='localtoGCS',
    python_callable=uploadtoGCS,
    provide_context=True,
    op_kwargs={'csv_name': CSV_NAME, 'folder_name': FOLDER_NAME},
    dag=dag)

#Function 3

# GCStoBQ = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
GCStoBQ = GCSToBigQueryOperator(
    task_id="GCStoBQ_partition",
    bucket= BUCKET_NAME,
    source_objects=[FOLDER_NAME +"/{}.csv".format(CSV_NAME)],
    destination_project_dataset_table= "{}.{}.{}".format(PROJECT_ID,DATASET_NAME,CSV_NAME),
    #Nunggu zaky----------
    source_format = 'csv',
    skip_leading_rows=1,
    field_delimiter=',',
    # autodetect=True,
    schema_fields=[
        {'name': 'id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'date', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'age', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'job', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'marital', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'education', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'default', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'housing', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'loan', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'contact', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'month', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'day_of_week', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'duration', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'campaign', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'pdays', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'previous', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'poutcome', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'emp_var_rate', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'cons_price_idx', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'cons_conf_idx', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'euribor3m', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'nr_employed', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'y', 'type': 'STRING', 'mode': 'NULLABLE'},
    ],
    #Nunggu zaky----------
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_TRUNCATE',
    time_partitioning={
                        "type": "DAY",
                        # "expirationMs": string,
                        "field": "date"
                        # "requirePartitionFilter": boolean
                      },
    dag=dag)

uploadtoGCS_task >> GCStoBQ


#Function 4

# t1 = BigQueryOperator(
#     task_id='bq_write_to_github_agg',
#     use_legacy_sql=False,
#     write_disposition='WRITE_TRUNCATE',
#     allow_large_results=True,
#     bql='''
#     #standardSQL
#     SELECT
#       "{{ yesterday_ds_nodash }}" as date,
#       repo,
#       SUM(stars) as stars_last_28_days,
#       SUM(IF(_PARTITIONTIME BETWEEN TIMESTAMP("{{ macros.ds_add(ds, -6) }}")
#         AND TIMESTAMP("{{ yesterday_ds }}") ,
#         stars, null)) as stars_last_7_days,
#       SUM(IF(_PARTITIONTIME BETWEEN TIMESTAMP("{{ yesterday_ds }}")
#         AND TIMESTAMP("{{ yesterday_ds }}") ,
#         stars, null)) as stars_last_1_day,
#       SUM(forks) as forks_last_28_days,
#       SUM(IF(_PARTITIONTIME BETWEEN TIMESTAMP("{{ macros.ds_add(ds, -6) }}")
#         AND TIMESTAMP("{{ yesterday_ds }}") ,
#         forks, null)) as forks_last_7_days,
#       SUM(IF(_PARTITIONTIME BETWEEN TIMESTAMP("{{ yesterday_ds }}")
#         AND TIMESTAMP("{{ yesterday_ds }}") ,
#         forks, null)) as forks_last_1_day
#     FROM
#       `airflow-cloud-public-datasets.github_trends.github_daily_metrics`
#     WHERE _PARTITIONTIME BETWEEN TIMESTAMP("{{ macros.ds_add(ds, -27) }}")
#     AND TIMESTAMP("{{ yesterday_ds }}")
#     GROUP BY
#       date,
#       repo
#     ''',
#     destination_dataset_table='airflow-cloud-public-datasets.github_trends.github_agg${{ yesterday_ds_nodash }}',
#     dag=dag)


    # with dag:

#     uploadtoGCS_task = PythonOperator(
#         task_id='uploadtoGCS',
#         python_callable=uploadtoGCS,
#         provide_context=True,
#         op_kwargs={'csv_name': CSV_NAME, 'folder_name': FOLDER_NAME},
#     )
