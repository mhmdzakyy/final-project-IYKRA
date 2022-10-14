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

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
# from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator, BigQueryCreateEmptyTableOperator
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
CSV_NAME = "bank_{}".format(yesterday)
# CSV_NAME = "bank_2022-10-01"

PROJECT_ID = "data-fellowship7"
DATASET_NAME = "final_project"

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

start = DummyOperator(task_id="start", retries=2, dag=dag)

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


GCStoBQ = GCSToBigQueryOperator(
    task_id="GCStoBQ_partition",
    bucket= BUCKET_NAME,
    source_objects=[FOLDER_NAME +"/{}.csv".format(CSV_NAME)],
    destination_project_dataset_table= "{}.{}.{}".format(PROJECT_ID,DATASET_NAME,CSV_NAME),
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
        {'name': 'defaults', 'type': 'STRING', 'mode': 'NULLABLE'},
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
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_TRUNCATE',
    time_partitioning={
                        "type": "DAY",
                        # "expirationMs": string,
                        "field": "date"
                        # "requirePartitionFilter": boolean
                      },
    dag=dag)

ct_bank_client_data = BigQueryCreateEmptyTableOperator(
    task_id="ct_bank_client_data",
    dataset_id=DATASET_NAME,
    table_id="f_bank_client_data",
    schema_fields=[
        {'name': 'id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'date', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'age', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'job', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'marital', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'education', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'defaults', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'housing', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'loan', 'type': 'STRING', 'mode': 'NULLABLE'},
    ],
    dag=dag)

ct_bank_social_economics = BigQueryCreateEmptyTableOperator(
    task_id="ct_bank_social_economics",
    dataset_id=DATASET_NAME,
    table_id="f_bank_social_economics",
    schema_fields=[
        {'name': 'id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'date', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'emp_var_rate', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'cons_price_idx', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'cons_conf_idx', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'euribor3m', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'nr_employed', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    ],
    dag=dag)

ct_related_last_contact = BigQueryCreateEmptyTableOperator(
    task_id="ct_related_last_contact",
    dataset_id=DATASET_NAME,
    table_id="f_related_last_contact",
    schema_fields=[
        {'name': 'id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'date', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'contact', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'month', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'day_of_week', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'duration', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    ],
    dag=dag)

ct_other_attibutes = BigQueryCreateEmptyTableOperator(
    task_id="ct_other_attibutes",
    dataset_id=DATASET_NAME,
    table_id="f_other_attibutes",
    schema_fields=[
        {'name': 'id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'date', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'campaign', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'pdays', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'previous', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'poutcome', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'y', 'type': 'STRING', 'mode': 'NULLABLE'},
    ],
    dag=dag)

load_bank_client_data = BigQueryOperator(
    task_id='load_bank_client_data',
    use_legacy_sql = False,
    write_disposition='WRITE_APPEND',
    sql=f" \
        SELECT \
            id, date, age, job, marital, education, defaults, housing, loan \
        FROM \
            `{PROJECT_ID}.{DATASET_NAME}.{CSV_NAME}`",
    destination_dataset_table=f'{PROJECT_ID}.{DATASET_NAME}.f_bank_client_data',

    dag=dag)

load_bank_social_economics = BigQueryOperator(
    task_id='load_bank_social_economics',
    use_legacy_sql = False,
    write_disposition='WRITE_APPEND',
    sql=f" \
        SELECT \
            id, date, emp_var_rate, cons_price_idx, cons_conf_idx, euribor3m, nr_employed \
        FROM \
            `{PROJECT_ID}.{DATASET_NAME}.{CSV_NAME}`",
    destination_dataset_table=f'{PROJECT_ID}.{DATASET_NAME}.f_bank_social_economics',

    dag=dag)

load_related_last_contact = BigQueryOperator(
    task_id='load_related_last_contact',
    use_legacy_sql = False,
    write_disposition='WRITE_APPEND',
    sql=f" \
        SELECT \
            id, date, contact, month, day_of_week, duration \
        FROM \
            `{PROJECT_ID}.{DATASET_NAME}.{CSV_NAME}`",
    destination_dataset_table=f'{PROJECT_ID}.{DATASET_NAME}.f_related_last_contact',

    dag=dag)

load_other_attibutes = BigQueryOperator(
    task_id='load_other_attibutes',
    use_legacy_sql = False,
    write_disposition='WRITE_APPEND',
    sql=f" \
        SELECT \
            id, date, campaign, pdays, previous, poutcome, y \
        FROM \
            `{PROJECT_ID}.{DATASET_NAME}.{CSV_NAME}`",
    destination_dataset_table=f'{PROJECT_ID}.{DATASET_NAME}.f_other_attibutes',

    dag=dag)

stop = DummyOperator(task_id="stop", retries=2, dag=dag)

start >> uploadtoGCS_task >> GCStoBQ >> [ct_bank_client_data, ct_bank_social_economics, ct_related_last_contact, ct_other_attibutes]
ct_bank_client_data >> load_bank_client_data  
ct_bank_social_economics >> load_bank_social_economics
ct_related_last_contact >> load_related_last_contact
ct_other_attibutes >> load_other_attibutes
[load_bank_client_data, load_bank_social_economics, load_related_last_contact, load_other_attibutes] >> stop