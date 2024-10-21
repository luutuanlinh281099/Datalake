import sys
from datetime import date, datetime

import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StringType, DecimalType
from lake.lib.writer import writer_scd4

    
def prequery_df_stg_product(df: DataFrame, date_str: str) -> DataFrame:
    df_output = df.filter(
        (F.col("cob_dt") == date_str)
    )
    return df_output
    
def prequery_df_crt_product(df: DataFrame, date: date) -> DataFrame:
    df_output = df.filter(
        (F.col("cob_dt") == date - 1)
    )
    return df_output

def get_records_delete(df1: DataFrame, df2: DataFrame) -> DataFrame:
    df_delete = df2.join(df_1
        cid,
        cif,
        "leftanti"
    )
    return df_delete
    
def get_records_update(df1: DataFrame, df2: DataFrame) -> DataFrame:
    df_same = df1.inersect(df2)
    df_update = df1.join(df_same
        cid,
        cif,
        "leftanti"
    )
    return df_update

    
def main():
    # set spark config
    spark = SparkSession.builder.appName("TestSparkJob").enableHiveSupport().getOrCreate()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    # set varriable from airflow
    data_date_str = sys.argv[1]
    data_date =  datetime.strptime(data_date_str, "%Y-%m-%d").date()

    # set data source
    df_stg_prf_product = spark.table("stg_prf_product")
    df_ctr_product_current = spark.table("ctr_product_current")

    # prequery data source and transform
    df_stg_prf_product = prequery_df_stg_product(
        df_stg_prf_product, 
        data_date_str
    )
    df_ctr_product_current = prequery_df_crt_product(
        df_ctr_product_current,
        date
    )
    df_delete = get_records_delete(
        df_stg_prf_product,
        df_ctr_product_current
    )
    df_update = get_records_update(
        df_stg_prf_product,
        df_ctr_product_current
    )

    # write in database
    writer(spark, df_stg_prf_product, df_delete, df_update, 'crt_product')
    spark.stop()

if _name_ == "_main_":
    main()