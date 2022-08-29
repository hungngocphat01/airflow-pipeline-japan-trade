from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Upload staging parquet files to HDFS
upload_staging_hdfs = BashOperator(
  task_id='upload_to_hdfs',
  bash_command='ls /var/staging/staged'
)