import sys
from datetime import date, datetime

import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StringType, DecimalType
from lake.lib.writer import writer_scd4

    
def prequery_df_crt_cif(df: DataFrame, date_str: str) -> DataFrame:
    df_output = df.filter(
        (F.col("cob_dt") == date_str)
    )
    return df_output
    
def prequery_df_ist_cif(df: DataFrame, date: date) -> DataFrame:
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
    df_crt_cif_current = spark.table("crt_cif_current")
    df_ist_cif_current = spark.table("ist_cif_current")

    # prequery data source and transform
    df_crt_cif_current = prequery_df_crt_cif(
        df_crt_cif_current, 
        data_date_str
    )
    df_ist_cif_current = prequery_df_ist_cif(
        df_ist_cif_current,
        date
    )
    df_delete = get_records_delete(
        df_crt_cif_current,
        df_ist_cif_current
    )
    df_update = get_records_update(
        df_crt_cif_current,
        df_ist_cif_current
    )

    # write in database
    writer(spark, df_crt_cif_current, df_delete, df_update, 'ist_cif')
    spark.stop()

if _name_ == "_main_":
    main()