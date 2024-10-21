import os
import pendulum
import datetime
from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.sensors.sql import SqlSensor
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.apache.kafka.operators.kafka_consumer import KafkaConsumerOperator
from lake.ultis.common import produce_kafka_message, consume_kafka_messages

file_path = 'lake/spark/spark_crt_product'
local = pendulum.local_timezone()


default_args = {
    'owner': 'datalake',
    'start_date': datetime(2024, 10, 19, tzinfo=local),
    'concurrency': 1,
    'max_active_runs': 1,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

with DAG('dag_crt_product',
        default_args=default_args,
        schedule_interval=None,
        user_defined_macros={
            'cob_dt_str': common.cob_dt_str,
            'produce_kafka_message': common.produce_kafka_message,
            'consume_kafka_messages': common.consume_kafka_messages,
        }
) as dag:

    get_topic = PythonOperator(
        task_id='get_topic',
        name='get_topic',
        python_callable=lambda: 'topic_datalake'
    )

    consume_kafka = KafkaConsumerOperator(
        task_id='consume_kafka',
        name='consume_kafka',
        bootstrap_servers='kafka:9092',
        topics=[get_topic.output],
        consumer_group=consumer_group,
        auto_offset_reset='earliest',
        value_deserializer=lambda m: m.decode('utf-8'),
        on_message_callback=consume_kafka_messages('mesage_crt_product'),
    )

    check_source = SqlSensor(
        task_id='check_source',
        name='check_source',
        poke_interval=15 * 60,
        mode='reschedule',
        timeout=6 * 60 * 60,
        soft_fail=False,
        conn_id="datalake",
        retries=2,
        sql="""
            SELECT COUNT(*) FROM (
                (SELECT DISTINCT TABLE_NAME
                FROM PROCCES_LOG
                WHERE COB_DT = '{{cob_dt_str(execution_date)}}'
                AND STATUS = 'S'
                AND TABLE_NAME IN (
                    SELECT TABLE_NAME_BEFORE
                    FROM TABLE_DEPEN
                    WHERE TABLE_NAME_AFTER = 'crt_product'))
                EXCEPT
                (SELECT TABLE_NAME_BEFORE
                FROM TABLE_DEPEN
                WHERE TABLE_NAME_AFTER = 'crt_product')
            )
        """,
    )

    submit_spark_job = SparkSubmitOperator(
        conn_id='datalake',
        name='submit_spark_job',
        task_id='submit_spark_job',
        run_as_user='datalake',
        application=file_path,
        application_args=['{{cob_dt_str(execution_date)}}'],
        executor_cores=5,
        num_executors=5,
        executor_memory="8G",
        driver_memory="8G",
        dag=dag,
    )

    insert_log = SparkSqlOperator(
        task_id='insert_log',
        conn_id='datalake',
        name='insert_log',
        run_as_user='datalake',
        executor_cores=2,
        num_executors=2,
        executor_memory="5G",
        driver_memory="8G",
        sql="""
            INSERT INTO PROCCES_LOG(
                1 AS job_di,
                'crt_product' AS table_name,
                {{cob_dt_str(execution_date)}} AS cob_dt,
                's' AS status
            )
        """
    )
    
    trino_query = TrinoOperator(
        task_id='trino_query',
        name='trino_query'
        trino_conn_id='trino_default',
        sql="""
            SELECT TABLE_NAME_AFTER FROM TABLE_DEPEN WHERE TABLE_NAME_BEFORE = 'crt_product'
        """,
        output_field="results"
    )

    produce_kafka = PythonOperator(
        task_id='produce_kafka',
        name='produce_kafka',
        python_callable=produce_kafka_message('results'),
    )

    get_topic >> consume_kafka >> check_source >> submit_spark_job >> insert_log >> trino_query >> produce_kafka