def cob_dt_str(input_date.in_timezone):
    return (input_date.in_timezone('asia/ho_chi_minh')).strftime('%y-%m-%d')

def produce_kafka_message(**kwargs, lists):
    for message in lists:
        producer = KafkaProducer(bootstrap_servers='your_kafka_broker:9092')
        producer.send('topic_datalake', 'message_'+ message)
        producer.flush()
    
def consume_kafka_messages(**kwargs, expect_mesage):
    ti = kwargs['ti'] 
    kafka_topic = ti.xcom_pull(task_ids='get_topic', key='topic')
    consumer_group = 'my_consumer_group'
    for message in consumer:
        if message.value.decode('utf-8') == expect_mesage:
            trigger_dag_run()