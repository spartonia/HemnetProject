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
    'Hemnet_daily_forsale_workflow',
    default_args=default_args,
    description='Pipeline for scraping daily "forsale" data from hemnet and \
        ingesting to deltalake on S3',
    schedule_interval='1 15 * * *' # 1:53 AM
)

cmd = """
    dailyspider \
    -a target='forsale' \
    -a fordate={{ ds }} \
    -s KAFKA_PRODUCER_TOPIC={{ var.value.KAFKA_TOPIC_FORSALE }} \
    -s KAFKA_PRODUCER_BROKERS={{ var.value.KAFKA_BROKERS }}
"""

scrape_pages_to_kafka = DockerOperator(
    task_id='hemnet_daily_forsale_spider',
    image=DAILY_SPIDER_DOCKER_IMAGE,
    command=cmd,
    docker_url='unix://var/run/docker.sock',
    network_mode='host',
    dag=dag
)


spark_submit_cmd_forsale_bronze = """
cd {{ var.value.ETL_HOME }}
{{ var.value.SPARK_HOME }}/spark-submit \
    --packages io.delta:delta-core_2.12:0.7.0,org.apache.hadoop:hadoop-aws:2.7.7,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0  \
    --conf spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore  \
    --conf spark.hadoop.fs.s3a.endpoint={{ var.value.S3_ENDPOINT }}  \
    --conf spark.driver.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true  \
    --conf spark.executor.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true  \
    --conf spark.hadoop.fs.s3a.access.key={{ var.value.AWS_S3_ACCESS }}  \
    --conf spark.hadoop.fs.s3a.secret.key={{ var.value.AWS_S3_SECRET }} \
    --py-files=dist/jobs.zip,dist/libs.zip dist/main.py  \
    --job dailyKafkaToBronze  \
    --job-args REDIS_HOST={{ var.value.REDIS_HOST }}  \
        KAFKA_TOPIC={{ var.value.KAFKA_TOPIC_FORSALE }}  \
        S3_SINK={{ var.value.S3_SINK_FORSALE_BRONZE }}  \
        TARGET=forsale
"""

kafka_to_bronze = SSHOperator(
    ssh_conn_id='ssh_alp-XPS-13-9380',
    task_id=f'kafkaForsaleToBronzeTask',
    command=spark_submit_cmd_forsale_bronze,
    dag=dag)


spark_submit_cmd_forsale_silver = """
cd {{ var.value.ETL_HOME }}
{{ var.value.SPARK_HOME }}/spark-submit \
    --packages io.delta:delta-core_2.12:0.7.0,org.apache.hadoop:hadoop-aws:2.7.7,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0  \
    --conf spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore  \
    --conf spark.hadoop.fs.s3a.endpoint={{ var.value.S3_ENDPOINT }}  \
    --conf spark.driver.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true  \
    --conf spark.executor.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true  \
    --conf spark.hadoop.fs.s3a.access.key={{ var.value.AWS_S3_ACCESS }}  \
    --conf spark.hadoop.fs.s3a.secret.key={{ var.value.AWS_S3_SECRET }} \
    --py-files=dist/jobs.zip,dist/libs.zip dist/main.py  \
    --job forsaleRefinedSilver  \
    --job-args  \
        S3_SOURCE={{ var.value.S3_SINK_FORSALE_BRONZE }}  \
        S3_SINK={{ var.value.S3_SINK_FORSALE_SILVER }}  \
        FOR_DATE={{ tomorrow_ds }}
"""

bronze_to_silver = SSHOperator(
    ssh_conn_id='ssh_alp-XPS-13-9380',
    task_id='forsaleToSilverTask',
    command=spark_submit_cmd_forsale_silver,
    dag=dag)


scrape_pages_to_kafka >> kafka_to_bronze  >> bronze_to_silver
