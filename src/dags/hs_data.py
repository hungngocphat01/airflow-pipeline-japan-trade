import os
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# INITIALIZE

STAGING_PATH = os.environ['AIRFLOW_STAGING_PATH'] or '/var/staging'
url = 'https://github.com/datasets/harmonized-system/'

HS_DATA_PATH = os.path.join(STAGING_PATH, 'hs_data')

# DEFINE CALLABLES

def process_hs_data_callable(ti):
  import pandas as pd

  # Process hs table
  hs_df = pd.read_csv(os.path.join(HS_DATA_PATH, 'data/harmonized-system.csv'))
  hs_df = hs_df[~(hs_df.section == 'TOTAL')]
  hs_df.to_parquet(os.path.join(STAGING_PATH, 'staged', 'hs_stage.parquet'), index=False)

  # Process sections table
  sec_df = pd.read_csv(os.path.join(HS_DATA_PATH, 'data/sections.csv'))
  sec_df.to_parquet(os.path.join(STAGING_PATH, 'staged', 'sections_stage.parquet'), index=False)

# DEFINE OPERATORS

download_hs_data = BashOperator(
  task_id='download_hs_data', 
  bash_command=f'rm -rf {HS_DATA_PATH} && git clone {url} {HS_DATA_PATH}',
)

process_hs_data = PythonOperator(
  task_id='process_hs_data',
  python_callable=process_hs_data_callable
)
