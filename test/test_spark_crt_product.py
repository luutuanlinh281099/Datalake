import sys
from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window


# get count
def pre_count(df1: DataFrame, df2: DataFrame, date_str: str) -> dict:
    count_source_all = df1.filter(F.col("cob_dt") == date_str).count()
    count_target_all = df12.filter(F.col("cob_dt") == date_str).count()
    count_source_key = df1.filter(
        F.col("cob_dt") == date_str
    ).dropDuplicates(["cid"]).count()
    count_target_key = df2.filter(
        F.col("cob_dt") == date_str
    ).dropDuplicates(["cid"]).count()
    return {
        "count_source_all": count_source_all,
        "count_target_all": count_target_all,
        "count_source_key": count_source_key,
        "count_target_key": count_target_key,
    }

# check count
def check_count_table(dict_count: dict):
    print(dict_count)
    if dict_count['count_source_all'] != dict_count['count_target_all']:
        raise ValueError("Value error, extract source is fail")
    elif dict_count['count_source_key'] != dict_count['count_target_key']:
        raise ValueError("Value error, column key not success")

# check column
def check_column_table(df: DataFrame, date_str: str):
    list_columns = df.columns
    for col in list_columns:
        count_col = df.filter(
            (F.col("cob_dt") == date_str) &
            (F.col(col).isNotNull())
        ).count()
        if count_col == 0:
            raise ValueError(f"Column {col} has full null value")

def main():
    # set spark config
    spark = SparkSession.builder.appName("TestSparkJob").enableHiveSupport().getOrCreate()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    
    # set data source
    df_table_source = spark.table("stg_prf_product")
    df_table_target = spark.table("crt_product_current")
    data_date_str = sys.argv[1]

    # transform data
    dict_col = pre_count(df_table_source, df_table_target, data_date_str)
    
    try:
        check_count_table(dict_col)
        check_column_table(df_table_target, data_date_str)
    except ValueError as error:
        print(error)
    finally:
        print('Success')
        spark.stop()

if _name_ == "_main_":
    main()