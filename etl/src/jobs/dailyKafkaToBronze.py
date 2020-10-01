import json
import redis

from datetime import datetime

import pyspark.sql.functions as F
from pyspark.sql.window import Window


VALID_TARGETS = ['forsale', 'sold', 'canonical']


def analyze(spark, **kwargs):

    redis_host = kwargs.get('REDIS_HOST')
    if not redis_host:
        raise Exception("'REDIS_HOST' is required. You can pass it through '--job-args'")

    redis_port = kwargs.get('REDIS_PORT', 6379)
    redis_client = redis.Redis(host=redis_host, port=redis_port)

    kafka_topic = kwargs.get('KAFKA_TOPIC')
    if not kafka_topic:
        raise Exception("'KAFKA_TOPIC' is required. You can pass it through '--job-args'")

    s3_bucket = kwargs.get('S3_SINK')
    if not s3_bucket:
        raise Exception("'S3_SINK' is required. You can pass it through '--job-args'")

    target = kwargs.get('TARGET')
    if not target or target not in VALID_TARGETS:
        raise ValueError(f"'TARGET' is required or is invalid. Valid hoices are {VALID_TARGETS}")

    redis_key_path = f"hemnet:{target}:kafka"
    p_offset_str = redis_client.hget(redis_key_path, kafka_topic)

    if p_offset_str:
        p_offset = json.loads(p_offset_str)
    else:
        p_offset = {'0': -2}

    tpo = {kafka_topic: p_offset}

    KAFKA_BROKERS = kwargs.get('KAFKA_BROKERS', 'localhost:9092')  # on host machine

    df = (spark
        .read
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", json.dumps(tpo))
        .option("kafka.group.id", f"spark.{target}.job")
        .load())

    today = F.current_date()

    data = (df
        .withColumn("key", df.key.cast("string"))
        .withColumn("value", df.value.cast("string"))
        .withColumn("ingestion_date", today))

    windowSpec = (Window
        .partitionBy(df["topic"], df["partition"]).orderBy(df["offset"].desc()))

    maxOffset = F.row_number().over(windowSpec)
    tpos = (df.withColumn("rowNumber", maxOffset)
        .where(F.col("rowNumber") == 1)
        .select("topic", "partition", "offset")
        .collect())

    (data
        .write
        .partitionBy("ingestion_date")
        .format("delta")
        .mode("append")
        # .option("overwriteSchema", "true")
        .save(s3_bucket))

    print('*' * 50)
    update_topic_partition(redis_client, tpos, redis_key_path)

    print("data count: ", data.count())
    print('*' * 50)


def update_topic_partition(redis, tpos, key_path):
    """
    Updates last read topicpartition offsets in redis
    """
    from itertools import groupby

    key_func = lambda x: x['topic']
    for topic, group in groupby(tpos, key_func):
        po = {}
        for el in group:
            po[str(el['partition'])] = int(el['offset'] + 1)

        po_str = json.dumps(po)
        redis.hmset(key_path, {topic: po_str})
