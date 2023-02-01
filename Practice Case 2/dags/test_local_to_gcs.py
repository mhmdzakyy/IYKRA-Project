from airflow import DAG
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

import pandas as pd
import numpy as np
import os
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

PROJECT_ID = "Data-fellowship7"
BUCKET = "fellowship7-finalproject"
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

def simpleNumpyToGCS(csv_name: str, folder_name: str,
                   bucket_name=BUCKET, **kwargs):
    hook = GoogleCloudStorageHook()
    
    # data = {'col1': [1, 2], 'col2': [3, 4]}

    # df = pd.DataFrame(data=data)
        
    # df.to_csv('example1.csv', index=False)

    hook.upload(bucket_name, 
                object_name='{}/{}.csv'.format(folder_name, csv_name), 
                filename="/.google/credentials/bank-additional-full.csv", 
                mime_type='text/csv')


dag = DAG('local_gcs_iykra_zaky',
          default_args=default_args,
          catchup=False)

with dag:

    simpleNumpyToGCS_task = PythonOperator(
        task_id='simpleNumpyToGCS',
        python_callable=simpleNumpyToGCS,
        provide_context=True,
        op_kwargs={'csv_name': 'airtravel', 'folder_name': 'mentoring1'},
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/mentoring1/bank-additional-full.csv"],
            },
        },
    )

    simpleNumpyToGCS_task >> bigquery_external_table_task