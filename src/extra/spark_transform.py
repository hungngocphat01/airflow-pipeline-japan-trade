import os
import sys
import pyspark.sql.functions as f
from pyspark.sql import SparkSession


def load_transform_fact(spark):
  # Read fact table from hdfs
  df = spark.read.csv('hdfs:///custom_1988_2020.csv')

  # Redefine schema
  new_df = df\
    .withColumn('year', f.substring('_c0', 0, 4).astype('int'))\
    .withColumn('month', f.substring('_c0', 4, 2).astype('int'))\
    .withColumn('exp_imp', f.when(f.col('_c1') == '1', 'export').otherwise('import'))\
    .withColumn('country_id', f.col('_c2').astype('int'))\
    .withColumn('hs6_code', f.substring('_c4', 0, 6))\
    .withColumn('q1', f.col('_c5').astype('int'))\
    .withColumn('q2', f.col('_c6').astype('int'))\
    .withColumn('value', f.col('_c7').astype('int'))\
    .select('year', 'month', 'country_id', 'exp_imp', 'hs6_code', 'q1', 'q2', 'value')

  # Write fact table to hive
  new_df.write.partitionBy('year').mode('overwrite').saveAsTable('fact_customs')


def load_transform_dim(spark, bucket_name):
  # Load dimension table
  hs_df = spark.read.parquet(f'gs://{bucket_name}/japan_trade_staged/hs_stage.parquet')
  sec_df = spark.read.parquet(f'gs://{bucket_name}/japan_trade_staged/sections_stage.parquet')

  # Save to hive
  hs_df.write.mode('overwrite').saveAsTable('dim_hs')
  sec_df.write.mode('overwrite').saveAsTable('dim_hs_sections')


def main():
  spark = SparkSession\
    .builder\
    .enableHiveSupport()\
    .master('yarn')\
    .appName('data-transform')\
    .getOrCreate() 

  STORAGE_BUCKET = sys.argv[1]

  load_transform_fact(spark)
  load_transform_dim(spark, STORAGE_BUCKET)


if __name__ == '__main__':
  main()

