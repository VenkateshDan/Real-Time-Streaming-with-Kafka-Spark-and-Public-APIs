from kafka import KafkaProducer, KafkaConsumer
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json

def produce_to_kafka():
    producer = KafkaProducer(
        bootstrap_servers='kafka:9092',
        value_serializer=lambda m: json.dumps(m).encode('utf-8')
    )
    test_message = {'key': 'value', 'timestamp': str(datetime.now())}
    producer.send('test-topic', test_message)
    producer.flush()
    print(f"Produced message: {test_message}")

def consume_from_kafka():
    consumer = KafkaConsumer(
        'test-topic',
        bootstrap_servers='kafka:9092',
        auto_offset_reset='earliest',  # Start from the beginning
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=5000  # Wait max 5 seconds for messages
    )
    print("Listening for messages...")
    for msg in consumer:
        print(f"Consumed message: {msg.value}")
        break  # Exit after first message for testing
    else:
        print("No messages found!")
    consumer.close()

default_args = {
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

with DAG(
    'dag_test_kafka',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    # schedule_interval=None,
    #schedule_interval='*/10 * * * *',
    schedule_interval='@hourly',
    catchup=False
) as dag:
    produce_task = PythonOperator(
        task_id='produce_message',
        python_callable=produce_to_kafka
    )
    
    consume_task = PythonOperator(
        task_id='consume_message',
        python_callable=consume_from_kafka
    )

    produce_task >> consume_task  
