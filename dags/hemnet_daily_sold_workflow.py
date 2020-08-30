from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.docker_operator import DockerOperator
from airflow.contrib.operators.ssh_operator import SSHOperator


DAILY_SPIDER_DOCKER_IMAGE = 'hemnet-daily-spider:latest'

default_args = {
        'owner'                 : 'airflow',
        'depend_on_past'        : False,
        'start_date'            : datetime(2020, 8, 29),
        'email_on_failure'      : False,
        'email_on_retry'        : False,
        'retries'               : 1,
        'retry_delay'           : timedelta(hours=1)
}

dag = DAG(
    'Hemnet_daily_sold_workflow',
    default_args=default_args,
    description='Pipeline for scraping daily "sold" data from hemnet and \
        ingesting to deltalake on S3',
    schedule_interval='23 20 * * *' # 8:23 PM
)

cmd = """
    dailyspider \
    -a target='sold' \
    -a fordate={{ ds }} \
    -s KAFKA_PRODUCER_TOPIC={{ var.value.KAFKA_TOPIC_SOLD }} \
    -s KAFKA_PRODUCER_BROKERS={{ var.value.KAFKA_BROKERS }}
"""

scrape_pages_to_kafka = DockerOperator(
    task_id='hemnet_daily_sold_spider',
    image=DAILY_SPIDER_DOCKER_IMAGE,
    command=cmd,
    docker_url='unix://var/run/docker.sock',
    network_mode='host',
    dag=dag
)

scrape_pages_to_kafka