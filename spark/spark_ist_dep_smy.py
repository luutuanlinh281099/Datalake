import sys
from datetime import date, datetime

import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StringType, DecimalType
from lake.lib.writer import writer_fact_append


def prequery_df_crt_dep_dtls(df: DataFrame, df_calender :DataFrame, date_str: str) -> DataFrame:
    df1 = df.filter(
        (F.col("cob_dt") == date_str)
    )
    df2 = df.filter(
        (F.col("cob_dt") == date_str -1)
    )
    df = df1.join(df2
        cid,
        cif,
        'left'
    ).join(df_calender
        cob_dt,
        left
    )
    return df_output

# transform
def transform(df: DataFrame, date_str: str) -> DataFrame:
    df_transform = df.withColumn(
        'mtd_casa_avg',
        F.when(F.col('first_month') == 1, F.col("dly_casa"))
        .otherwise((F.col('mtd_casa_avg') * F.coL('number_of_month') - 1) + F.col("dly_casa")) / F.coL('number_of_month'))
    ).withColumn(
        'qtd_casa_avg',
        F.when(F.col('first_quater') == 1, F.col("dly_casa"))
        .otherwise((F.col('qtd_casa_avg') * F.coL('number_of_quater') - 1) + F.col("dly_casa")) / F.coL('number_of_quater'))
    ).withColumn(
        'ytd_casa_avg',
        F.when(F.col('first_year') == 1, F.col("dly_casa"))
        .otherwise((F.col('ytd_casa_avg') * F.coL('number_of_year') - 1) + F.col("dly_casa")) / F.coL('number_of_year'))
    ).withColumn(
        'mtd_funs_transfer',
        F.when(F.col('first_month') == 1, F.col("dly_funs_transfer"))
        .otherwise(F.col('mtd_funs_transfer') + F.col("dly_funs_transfer"))
    ).withColumn(
        'qtd_funs_transfer',
        F.when(F.col('first_quater') == 1, F.col("dly_funs_transfer"))
        .otherwise(F.col('qtd_funs_transfer') + F.col("dly_funs_transfer"))
    ).withColumn(
        'ytd_funs_transfer',
        F.when(F.col('first_year') == 1, F.col("dly_funs_transfer"))
        .otherwise(F.col('ytd_funs_transfer') + F.col("dly_funs_transfer"))
    ).withColumn(
        'dly_all_deposit',
        F.col('dly_casa') + F.col('dly_short_term_deposit') + F.col('dly_long_term_deposit')
    ).withColumn(
        'mtd_all_deposit',
        F.when(F.col('first_month') == 1, F.col("dly_all_deposit"))
        .otherwise(F.col('mtd_all_deposit') + F.col('dly_all_deposit'))
    ).withColumn(
        'qtd_all_deposit',
        F.when(F.col('first_quater') == 1, F.col("dly_all_deposit"))
        .otherwise(F.col('qtd_all_deposit') + F.col('dly_all_deposit'))
    ).withColumn(
        'ytd_all_deposit',
        F.when(F.col('first_year') == 1, F.col("dly_all_deposit"))
        .otherwise(F.col('ytd_all_deposit') + F.col('dly_all_deposit'))
    ).withColumn(
        'mtd_interest_rate_avg',
        F.when(F.col('first_month') == 1, F.col("dly_casa"))
        .otherwise((F.col('mtd_interest_rate_avg') * F.coL('number_of_month') - 1) + F.col("dly_interest_rate")) / F.coL('number_of_month'))
    ).withColumn(
        'qtd_interest_rate_avg',
        F.when(F.col('first_quater') == 1, F.col("dly_casa"))
        .otherwise((F.col('qtd_interest_rate_avg') * F.coL('number_of_quater') - 1) + F.col("dly_interest_rate")) / F.coL('number_of_quater'))
    ).withColumn(
        'ytd_interest_rate_avg',
        F.when(F.col('first_year') == 1, F.col("dly_casa"))
        .otherwise((F.col('ytd_interest_rate_avg') * F.coL('number_of_year') - 1) + F.col("dly_interest_rate")) / F.coL('number_of_year'))
    ).withColumn(
        'mtd_profits',
        F.when(F.col('first_month') == 1, F.col("dly_all_deposit"))
        .otherwise(F.col('mtd_profits') + F.col('dly_profits'))
    ).withColumn(
        'qtd_profits',
        F.when(F.col('first_quater') == 1, F.col("dly_all_deposit"))
        .otherwise(F.col('qtd_profits') + F.col('dly_profits'))
    ).withColumn(
        'ytd_profits',
        F.when(F.col('first_year') == 1, F.col("dly_all_deposit"))
        .otherwise(F.col('ytd_profits') + F.col('dly_profits'))
    ).select(
        F.col('cif'),
        F.col('cid'),
        F.col('tyoe'),
        F.col('close_date'),
        F.col('dly_case'),
        F.col('mtd_casa_avg'),
        F.col('qtd_casa_avg'),
        F.col('ytd_casa_avg'),
        F.col('dly_funs_transfer'),
        F.col('mtd_funs_transfer'),
        F.col('qtd_funs_transfer'),
        F.col('ytd_funs_transfer'),
        F.col('dly_all_deposit'),
        F.col('mtd_all_deposit'),
        F.col('mtd_all_deposit'),
        F.col('mtd_all_deposit'),
        F.col('dly_interest_rate'),
        F.col('mtd_interest_rate_avg'),
        F.col('qtd_interest_rate_avg'),
        F.col('ytd_interest_rate_avg'),
        F.col('dly_profits'),
        F.col('mtd_profits'),
        F.col('qtd_profits'),
        F.col('ytd_profits'),
        F.col('nbr_remain_day'),
        F.col('es_profits'),
        F.col('cob_dt'),        
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
    df_crt_dep_dtls = spark.table("crt_dep_dtls")
    df_calender = spark.table("calender")

    # prequery data source and transform
    df_join = prequery_df_crt_dep_dtls(
        df_crt_dep_dtls, 
        df_calender,
        data_date_str
    )
    df_transform = transform(
        df_join,
        data_date_str
    )

    # write in database
    writer_fact_append(spark, df_transform, 'ist_dep_smy', data_date_str)
    spark.stop()

if _name_ == "_main_":
    main()