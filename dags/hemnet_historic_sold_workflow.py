from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.docker_operator import DockerOperator
from airflow.contrib.operators.ssh_operator import SSHOperator


HEMNET_SPIDER_DOCKER_IMAGE = 'hemnet-spiders:latest'

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
    'Hemnet_historic_sold_workflow',
    default_args=default_args,
    description='Pipeline for scraping daily "forsale" data from hemnet and \
        ingesting to deltalake on S3',
    schedule_interval='1 8 2 9 *' # At 08:01 on day-of-month 2 in September.
)

historic_sold_url_cmd = """
    historicSoldURLCollector \
    -s REDIS_HOST={{ var.value.REDIS_HOST }} \
    -a MAX_LOC_PER_RUN=50
"""

scrape_sold_urls_to_redis = DockerOperator(
    task_id='hemnet_historic_sold_urls_spider',
    image=HEMNET_SPIDER_DOCKER_IMAGE,
    command=historic_sold_url_cmd,
    docker_url='unix://var/run/docker.sock',
    network_mode='host',
    dag=dag
)

scrape_sold_urls_to_redis
