from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.docker_operator import DockerOperator


# XXX: Update image name/tag
docker_image_to_run = 'hemnet-forsalesspider:latest'
KAFKA_PRODUCER_TOPIC = 'forsale'
KAFKA_PRODUCER_BROKERS='192.168.86.27:9092'
REDIS_HOST='192.168.86.27'


default_args = {
        'owner'                 : 'airflow',
        'description'           : 'Hemnet scraper for sales items',
        'depend_on_past'        : False,
        'start_date'            : datetime(2020, 8, 12),
        'email_on_failure'      : False,
        'email_on_retry'        : False,
        'retries'               : 1,
        'retry_delay'           : timedelta(hours=1)
}

dag = DAG(
    'Hemnet_forsales_spider_dag',
    default_args=default_args,
    description='Docker operator for hemnet-forsalesspider',
    schedule_interval='53 15 * * *' # 3:53 PM
)

cmd = f"""
    forsalespider \
    -s KAFKA_PRODUCER_TOPIC={KAFKA_PRODUCER_TOPIC} \
    -s KAFKA_PRODUCER_BROKERS={KAFKA_PRODUCER_BROKERS} \
    -s REDIS_HOST={REDIS_HOST}
"""
t2 = DockerOperator(
    task_id='hemnet_forsalesspider',
    image=f'{docker_image_to_run}',
    command=cmd,
    docker_url='unix://var/run/docker.sock',
    network_mode='host',
    dag=dag
)

t2
