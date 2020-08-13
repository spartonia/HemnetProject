from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.docker_operator import DockerOperator


# XXX: Update image name/tag
docker_image_to_run = 'hemnet-forsalesspider:test'

default_args = {
        'owner'                 : 'airflow',
        'description'           : 'Hemnet scraper for sales items',
        'depend_on_past'        : False,
        'start_date'            : datetime.now() - timedelta(minutes=15),
        'email_on_failure'      : False,
        'email_on_retry'        : False,
        'retries'               : 1,
        'retry_delay'           : timedelta(minutes=5)
}

dag = DAG(
    'Hemnet_forsales_spider_dag',
    default_args=default_args,
    description='Demoing docker operator for hemnet-forsalesspider',
    schedule_interval=timedelta(hours=1)
)

cmd = """
    forsalespider \
    -s KAFKA_PRODUCER_TOPIC='test-topic' \
    -s KAFKA_PRODUCER_BROKERS=192.168.86.27:9092 \
    -s REDIS_HOST=192.168.86.27
"""
t2 = DockerOperator(
    task_id='hemnet_forsalesspider_test',
    image=f'{docker_image_to_run}',
    command=cmd,
    docker_url='unix://var/run/docker.sock',
    network_mode='host',
    dag=dag
)

t2
