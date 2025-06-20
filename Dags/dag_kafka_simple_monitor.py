from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaAdminClient, KafkaConsumer
from kafka.errors import KafkaError
from tabulate import tabulate

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_kafka_metrics():
    """Simplified Kafka monitoring - just topic names and message counts"""
    
    # Kafka connection config
    bootstrap_servers = 'kafka:9092'
    excluded_topics = ['__consumer_offsets', '__transaction_state']  # Skip internal topics
    
    try:
        # Initialize admin client
        admin = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            request_timeout_ms=10000
        )
        
        # Get all non-internal topics
        all_topics = admin.list_topics()
        topics = [t for t in sorted(all_topics) if t not in excluded_topics]
        
        print(f"\nMonitoring {len(topics)}/{len(all_topics)} topics (simplified view)\n")
        
        metrics_data = []
        
        for topic in topics:
            try:
                # Get message count using consumer offsets
                consumer = KafkaConsumer(
                    bootstrap_servers=bootstrap_servers,
                    enable_auto_commit=False,
                    auto_offset_reset='earliest'
                )
                
                partitions = consumer.partitions_for_topic(topic)
                if not partitions:
                    metrics_data.append([topic, 0])
                    continue
                
                # Get last offset (message count)
                consumer.seek_to_end()
                message_count = consumer.position(consumer.partitions_for_topic(topic).pop())
                
                metrics_data.append([topic, message_count])
                consumer.close()
                
            except Exception as e:
                print(f"Error getting count for {topic}: {str(e)}")
                metrics_data.append([topic, "ERROR"])
                continue
        
        admin.close()
        
        # Print simplified table
        headers = ["Topic Name", "Message Count"]
        print(tabulate(metrics_data, headers=headers, tablefmt="grid"))
        print("\nReport generated at:", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        
        return metrics_data
        
    except Exception as e:
        print(f"Failed to connect to Kafka: {str(e)}")
        raise

with DAG(
    'kafka_simple_monitor',
    default_args=default_args,
    description='Simplified Kafka topics monitoring - names and counts only',
    schedule_interval='0 * * * *',  # Run at the top of every hour
    catchup=False,
    tags=['kafka', 'monitoring'],
) as dag:

    get_metrics_task = PythonOperator(
        task_id='get_kafka_metrics',
        python_callable=get_kafka_metrics,
        execution_timeout=timedelta(minutes=5)
    )
