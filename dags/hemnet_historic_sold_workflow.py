from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.docker_operator import DockerOperator
from airflow.contrib.operators.ssh_operator import SSHOperator


HEMNET_SPIDER_DOCKER_IMAGE = 'hemnet-spiders:latest'

default_args = {
        'owner'                 : 'airflow',
        'depend_on_past'        : False,
        'start_date'            : datetime(2020, 9, 4),
        'email_on_failure'      : False,
        'email_on_retry'        : False,
        'retries'               : 1,
        'retry_delay'           : timedelta(hours=1)
}

dag = DAG(
    'Hemnet_historic_sold_workflow',
    default_args=default_args,
    description='Pipeline for scraping historic sold data from hemnet and \
        publishing to kafka',
    schedule_interval='27 0-18 * * *' # At minute 27 past every hour from 0 through 18.
)

# historic_sold_url_cmd = """
#     historicSoldURLCollector \
#     -s REDIS_HOST={{ var.value.REDIS_HOST }} \
#     -a MAX_LOC_PER_RUN=50
# """

# scrape_sold_urls_to_redis = DockerOperator(
#     task_id='hemnet_historic_sold_urls_spider',
#     image=HEMNET_SPIDER_DOCKER_IMAGE,
#     command=historic_sold_url_cmd,
#     docker_url='unix://var/run/docker.sock',
#     network_mode='host',
#     dag=dag
# )

# scrape_sold_urls_to_redis

historic_sold_downloader = """
    historicSoldSpider \
    -a MAX_ITEMS_PER_RUN=1200 \
    -s REDIS_HOST={{ var.value.REDIS_HOST }} \
    -s KAFKA_PRODUCER_TOPIC={{ var.value.KAFKA_TOPIC_SOLD }} \
    -s KAFKA_PRODUCER_BROKERS={{ var.value.KAFKA_BROKERS }}
"""

scrape_pages_to_kafka = DockerOperator(
    task_id='hemnet_historic_sold_downloader_spider',
    image=HEMNET_SPIDER_DOCKER_IMAGE,
    command=historic_sold_downloader,
    docker_url='unix://var/run/docker.sock',
    network_mode='host',
    dag=dag
)

scrape_pages_to_kafka
