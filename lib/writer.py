def writer_fact_append(spark: SparkSession, df: DataFrame, table: str, date_str: str):
    spark.sql(f"""ALTER TABLE '{{table}}' DROP IF EXISTS PARTITION (cob_dt = '{date_str}')""")
    df.write.mode('append').insertInto('table')
    print("----- Insert success -----")

def writer_scd4(spark: SparkSession, df1: DataFrame, df2: DataFrame, df3: DataFrame, table: str):
    df.write.mode('overwrite').insertInto(table + '_curent')
    df_hist = df2.unionAll(df_3)
    df_hist.write.mode('append').insertInto(table + '_hist')
    print("----- Insert success -----")