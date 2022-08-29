import os
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

COUNTRY_CODE_URL = 'https://www.customs.go.jp/toukei/sankou/code/country_e.htm'
STAGING_PATH = os.environ['AIRFLOW_STAGING_PATH'] or '/var/staging'
COUNTRY_HTML_PATH = os.path.join(STAGING_PATH, 'countries.html')

# DEFINE CALLABLES

def process_country_data_callable(ti):
  import pandas as pd

  # Airflow task instance passed by the engine
  df = pd.read_html(COUNTRY_HTML_PATH)[0]
  
  # Make first row the header row
  df.columns = df.iloc[0]
  df = df.iloc[1:]

  # Convert Code column to int
  df['Code'] = df['Code'].astype(int)

  # Rename columns
  df.rename({'Geographical Zone': 'zone', 'Code': 'id', 'Country': 'name'}, axis=1, inplace=True)

  # Write to staging volume
  df.to_parquet(os.path.join(STAGING_PATH, 'staged', 'country_data_stage.parquet'), index=False)

# DEFINE OPERATORS

download_country_data = BashOperator(
  task_id='download_country_data',
  bash_command=f'curl -sSLf {COUNTRY_CODE_URL} > {COUNTRY_HTML_PATH}',
)

process_country_data = PythonOperator(
  task_id='process_country_data',
  python_callable=process_country_data_callable
)