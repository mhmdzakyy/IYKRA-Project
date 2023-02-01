from airflow import DAG
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

from google.cloud import storage

import pandas as pd
import numpy as np
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

dataset_file = "bank-additional-full.csv"
dataset_url = f"https://drive.google.com/file/d/192goyWajSF7o9SR-gZ9E_6WIz43IEyVa/view/{dataset_file}"
# path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
path_to_local_home = "/opt/airflow"
# parquet_file = dataset_file.replace('.csv', '.parquet')
# BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')



def simpleNumpyToGCS(csv_name: str, folder_name: str, 
                    bucket_name="fellowship7-finalproject", **kwargs):
    hook = GoogleCloudStorageHook()
    
    hook.upload(bucket_name, 
                object_name='{}/{}.csv'.format(folder_name, csv_name), 
                filename="/.google/credentials/bank-additional-full.csv", 
                mime_type='text/csv')

def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

dag = DAG('local_to_gcs_hook',
          default_args=default_args,
          catchup=False)

with dag:

    # simpleNumpyToGCS_task = PythonOperator(
    #     task_id='simpleNumpyToGCS',
    #     python_callable=simpleNumpyToGCS,
    #     provide_context=True,
    #     op_kwargs={'csv_name': 'bank-additional-full', 'folder_name': 'mentoring1'},
    # )

    # bigquery_external_table_task = BigQueryCreateExternalTableOperator(
    #     task_id="bigquery_external_table_task",
    #     table_resource={
    #         "tableReference": {
    #             "projectId": PROJECT_ID,
    #             "datasetId": BIGQUERY_DATASET,
    #             "tableId": "external_table",
    #         },
    #         "externalDataConfiguration": {
    #             "sourceFormat": "PARQUET",
    #             "sourceUris": [f"gs://{BUCKET}/raw/{parquet_file}"],
    #         },
    #     },
    # )

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSL {dataset_url} > {path_to_local_home}/{dataset_file}"
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": "fellowship7-finalproject",
            "object_name": f"raw/{dataset_file}",
            "local_file": f"{path_to_local_home}/{dataset_file}",
        },
    )

    download_dataset_task >> local_to_gcs_task
    # simpleNumpyToGCS_task