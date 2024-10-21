import sys
from datetime import date, datetime

import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StringType, DecimalType
from lake.lib.writer import writer_overwrite


def prequery_df_stg_dep(df: DataFrame, date_str: str) -> DataFrame:
    df = df.filter(
        (F.col("cob_dt") == date_str)
    ).withColumn(
        'dly_casa',
        F.when(F.col("type") == 'DDL', F.col("balance"))
        .otherwise(F.lit(0))
    ).withColumn(
        'dly_short_term_deposit',
        F.when(F.col("type") <= 12, F.col("balance"))
        .otherwise(F.lit(0))
    ).withColumn(
        'dly_long_term_deposit',
        F.when(F.col("type") > 12, F.col("balance"))
        .otherwise(F.lit(0))
    ).withColumn(
        'dly_profits',
        F.col("balance") * F.col("interset_rate")
    ).withColumn(
        'nbr_remain_day',
        F.col("close_date") - date_str
    ).withColumn(
        'es_profits',
        F.col("nbr_remain_day") * F.col("interest_rate")
    )
    df_output = df.groupBy(
        F.col("cif"),
        F.col("cid"),
        F.col("type"),
        F.col("close_date"),
        F.col("interest_rate")
    ).agg(
        F.sum(F.col("dly_casa")),
        F.sum(F.col("dly_short_term_deposit")),
        F.sum(F.col("dly_long_term_deposit")),
        F.sum(F.col("dly_profits")),
        F.sum(F.col("nbr_remain_day")),
        F.sum(F.col("es_profits"))
    )
    return df_output
    
def prequery_df_stg_dda_transaction(df: DataFrame, date_str: str) -> DataFrame:
    df = df.filter(
        (F.col("cob_dt") == date_str)
    ).withColumn(
        'dly_funs_transfer',
        F.when(F.col("from").isnull(), F.col("amount"))
        F.when(F.col("to").isnull(), - F.col("amount"))
        .otherwise(F.lit(0))
    )
    df_output = df.groupBy(
        F.col("cif"),
        F.col("cid"),
    ).agg(
        F.sum(F.col("dly_funs_transfer"))
    )
    return df_output

# transform
def transform(df1: DataFrame, df2: DataFrame, date_str: str) -> DataFrame:
    df_transform = df1.join(df2
        cid,
        cif,
        "left"
    ).select(
        F.col("cif"),
        F.col("cid"),
        F.col("type"),
        F.col("close_date"),
        F.col("interest_rate")
        F.col("dly_casa"),
        F.col("dly_funs_transfer"),
        F.col("dly_short_term_deposit"),
        F.col("dly_long_term_deposit"),
        F.col("dly_profits"),
        F.col("nbr_remain_day"),
        F.col("es_profits"),
        F.lit(date_str) as cob_dt
    )
    return df_transform
    
    
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
    df_stg_prf_dep = spark.table("stg_prf_dep")
    df_stg_prf_dda_transaction = spark.table("stg_prf_dda_transaction")

    # prequery data source and transform
    df_stg_prf_dep = prequery_df_stg_dep(
        df_stg_prf_dep, 
        data_date_str
    )
    df_stg_prf_dda_transaction = prequery_df_stg_dda_transaction(
        df_stg_prf_dda_transaction,
        data_date_str
    )
    df_transform = transform(
        df_stg_prf_dep,
        df_stg_prf_dda_transaction,
        data_date_str
    )

    # write in database
    writer_overwrite(spark, df_transform, 'crt_dep_dtls', data_date_str)
    spark.stop()

if _name_ == "_main_":
    main()