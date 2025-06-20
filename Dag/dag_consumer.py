from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='dag_consumer',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False,
    tags=['kafka', 'spark'],
) as dag:

    # Task: Run Kafka Producer
    run_kafka_producer = BashOperator(
        task_id='run_kafka_producer',
        bash_command='python3 /opt/airflow/dags/kafka_producer.py',
    )

    # Task: Run Spark Consumer
    run_spark_consumer = SparkSubmitOperator(
        task_id='run_spark_consumer',
        application='/opt/airflow/dags/spark_consumer.py',
        conn_id='spark_default',
        verbose=True,
        conf={
            'spark.executor.memory': '2g',
            'spark.driver.memory': '1g',
        },
    )

    run_kafka_producer >> run_spark_consumer
