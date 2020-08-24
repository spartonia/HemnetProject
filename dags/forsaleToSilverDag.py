from datetime import timedelta, datetime
import airflow
from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator


default_args = {
        'owner'                 : 'airflow',
        'description'           : 'Hemnet scraper for sales items',
        'depend_on_past'        : False,
        'start_date'            : datetime(2020, 8, 23),
        'email_on_failure'      : False,
        'email_on_retry'        : False,
        'retries'               : 0,
        'retry_delay'           : timedelta(hours=1)
}


dag = DAG(
    dag_id='forsaleToSilvereDag',
    default_args=default_args,
    description='Ingesting forsale data to S3 silver grade deltalake',
    schedule_interval='0 20 * * *',
)

spark_submit_cmd = """
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
        FOR_DATE={{ ds }}
"""

t1 = SSHOperator(
    ssh_conn_id='ssh_alp-XPS-13-9380',
    task_id='forsaleToSilverTask',  # TODO {{ ds }}
    command=spark_submit_cmd,
    dag=dag)

t1