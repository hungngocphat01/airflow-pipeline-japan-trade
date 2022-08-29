import os 
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

GCP_PROJECT_ID = os.environ['GCP_PROJECT_ID']
GCP_GCS_BUCKET = os.environ['GCP_GCS_BUCKET']
STAGING_PATH = os.environ['AIRFLOW_STAGING_PATH']
STAGED_PATH = os.path.join(STAGING_PATH, 'staged')

EXTRA_PATH = os.path.join(STAGING_PATH, 'extra')

# Copy spark job
copy_spark_app = BashOperator(
  task_id='copy_spark_job',
  bash_command=f'cp {EXTRA_PATH}/*.py {STAGED_PATH}'
)

# Upload staging parquet files to GCS
upload_staging_gcs = LocalFilesystemToGCSOperator(
  task_id='upload_staging_to_gcs',
  src=[os.path.join(STAGED_PATH, f) for f in os.listdir(STAGED_PATH)],
  dst='japan_trade_staged/',
  bucket=GCP_GCS_BUCKET,
  gcp_conn_id='gcp_connection'
)

GCP_DATAPROC_REGION = os.environ['GCP_DATAPROC_REGION']
GCP_DATAPROC_CLUSTER_NAME = os.environ['GCP_DATAPROC_CLUSTER_NAME']

# Run Spark job to transform the big dataset and ingest to hive
PYSPARK_JOB = {
  'reference': {'project_id': GCP_PROJECT_ID},
  'placement': {'cluster_name': GCP_DATAPROC_CLUSTER_NAME},
  'pyspark_job': {
    'main_python_file_uri': os.path.join('gs://', GCP_GCS_BUCKET, 'japan_trade_staged', 'spark_transform.py'),
    'args': [GCP_GCS_BUCKET]
  }
}

spark_transform_ingest = DataprocSubmitJobOperator(
  task_id='run_dataproc_spark',
  job=PYSPARK_JOB,
  location=GCP_DATAPROC_REGION,
  project_id=GCP_PROJECT_ID,
  gcp_conn_id='gcp_connection'
)