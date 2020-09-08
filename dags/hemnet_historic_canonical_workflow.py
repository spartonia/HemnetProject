from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.contrib.operators.ssh_operator import SSHOperator


HEMNET_SPIDER_DOCKER_IMAGE = 'hemnet-spiders:latest'

default_args = {
        'owner'                 : 'airflow',
        'depend_on_past'        : False,
        'start_date'            : datetime(2020, 9, 8),
        'email_on_failure'      : False,
        'email_on_retry'        : False,
        'retries'               : 1,
        'retry_delay'           : timedelta(minutes=5)
}

dag = DAG(
    'Hemnet_historic_canonical_workflow',
    default_args=default_args,
    description='Pipeline for scraping historic canonical urls data (expired forsale) \
        from hemnet and publishing to kafka',
    schedule_interval='*/17 * * * *' # At minute 17th minute.
)


historic_canonical_downloader = """
    historicCanonicalSpider \
    -a MAX_ITEMS_PER_RUN=1150 \
    -s REDIS_HOST={{ var.value.REDIS_HOST }} \
    -s KAFKA_PRODUCER_TOPIC={{ var.value.KAFKA_TOPIC_CANONICAL }} \
    -s KAFKA_PRODUCER_BROKERS={{ var.value.KAFKA_BROKERS }}
"""

scrape_pages_to_kafka = DockerOperator(
    task_id='hemnet_historic_canonical_downloader_spider',
    depends_on_past=True,
    wait_for_downstream=True,
    image=HEMNET_SPIDER_DOCKER_IMAGE,
    command=historic_canonical_downloader,
    docker_url='unix://var/run/docker.sock',
    network_mode='host',
    dag=dag
)

scrape_pages_to_kafka 
