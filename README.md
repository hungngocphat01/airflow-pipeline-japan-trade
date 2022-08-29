# (WIP) Apache Airflow pipeline and Spark/Hadoop practice

Projected final pipeline

![Expected pipeline](assets/draft_pipeline.jpeg)

## Data sources
- [Japanese trade statistics from 1988 to 2019](https://www.kaggle.com/datasets/zanjibar/100-million-data-csv) (~100M rows)
- [HS Code (2017)](https://github.com/datasets/harmonized-system/)
- [Japanese customs country code](https://www.customs.go.jp/toukei/sankou/code/country_e.htm)

## Technologies used 
- Apache Airflow (`docker-compose` taken from [data-engineering-zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp))
- Apache Spark
- Apache Hadoop 
- Google Cloud Services

## How to

### Setup GCS
1. Create the following objects in GCP:
   - A storage bucket.
   - A Dataproc cluster.
2. Download your GCP credential `json` file and place it in `~/.google/credentials/google_credentials.json`.

### Setup local Airflow environment
1. `cd src` 
2. `mkdir -p ./logs ./plugins`
3. `mv .env-template .env`
4. Open `.env` file and fill in neccessary information about your GCP project and storage bucket.
5. `docker compose build`
6. `docker compose up`
7. Go to `localhost:8080` (Airflow Web UI), open **Admin > Connection** and add a **Google Cloud** connection with the following information: 
   - Name: `gcp_connection`
   - Credential file: `/.google/credentials/google_credentials.json`
   - Project ID: `<YOUR PROJECT ID>`

### Run the pipeline 
1. Manually load the file `custom_1988_2020.csv` (from the _first_ dataset listed above) to `gs://japan_trade_staged/` (due to practical limitations of my Internet connection, this should be done manually).
2. Run the DAG from the local Airflow dashboard.
3. Done 

### Manual works 
The data has been transformed and stored to Apache Hive for further analytics. Below steps are expected to be done manually.

1. Some analytics with PySpark
2. A simple MapReduce Job