from concurrent.futures import process
import os 
from datetime import datetime

from airflow import DAG 
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import country_data
import hs_data
import gcp

STAGING_PATH = os.environ['AIRFLOW_STAGING_PATH']
STAGED_PATH = os.path.join(STAGING_PATH, 'staged')

with DAG(
  'japan_trade_pipeline',
  start_date=datetime.now()
) as dag:
  prepare_fs = BashOperator(
    task_id='prepare_fs',
    bash_command=f'if [[ ! -d "{STAGED_PATH}" ]]; then mkdir -p "{STAGED_PATH}"; fi'
  )

  download_country_data = country_data.download_country_data
  download_hs_data = hs_data.download_hs_data
  download_custom_data = ...

  process_country_data = country_data.process_country_data
  process_hs_data = hs_data.process_hs_data
  upload_to_gcs = gcp.upload_staging_gcs

  prepare_fs >> download_country_data >> process_country_data >> upload_to_gcs
  prepare_fs >> download_hs_data >> process_hs_data >> upload_to_gcs