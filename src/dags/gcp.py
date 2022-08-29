import os 
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

GCP_PROJECT_ID = os.environ['GCP_PROJECT_ID']
GCP_GCS_BUCKET = os.environ['GCP_GCS_BUCKET']
STAGING_PATH = os.environ['AIRFLOW_STAGING_PATH']
STAGED_PATH = os.path.join(STAGING_PATH, 'staged')

# Upload staging parquet files to GCS
upload_staging_gcs = LocalFilesystemToGCSOperator(
  task_id='upload_to_gcs',
  src=[os.path.join(STAGED_PATH, f) for f in os.listdir(STAGED_PATH)],
  dst='japan_trade_staged/',
  bucket=GCP_GCS_BUCKET,
  gcp_conn_id='gcp_connection'
)