"""
Connect to kafka
While there is data in kafka:
    ingest to delta lake
sc.stop gracefully

df.select("offset").orderBy($"offset".desc).take(3)
get the latest offset from redis
read from there to now
update the redis entry for last read offset
group by topic, partition, offset

val df = spark.read.format("kafka")
.option("kafka.bootstrap.servers", "localhost:9092")
.option("subscribe", "test-topic")
.option("startingOffsets", \"""{"test-topic": {"0": 23}}\""").load()


val windowSpec = Window.partitionBy(df("topic"), df("partition")).orderBy($"offset".desc)
val maxOffset =  row_number.over(windowSpec)
df.withColumn("rowNumber", maxOffset).where($"rowNumber" === 1).select("topic", "partition", "offset").show(5)
"""
import json
import redis

import pyspark.sql.functions as F
from pyspark.sql.window import Window


REDIS_HOST = 'localhost'
REDIS_PORT = 6379
KAFKA_TOPIC = "test-topic"

redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)


def analyze(spark):

    p_offset_str = redis_client.hget("hemnet:forsale:kafka", KAFKA_TOPIC)
    if p_offset_str:
        p_offset = json.loads(p_offset_str)
    else:
        p_offset = {'0': -2}

    tpo = {KAFKA_TOPIC: p_offset}

    df = spark\
        .read\
        .format("kafka")\
        .option("kafka.bootstrap.servers", "localhost:9092")\
        .option("subscribe", KAFKA_TOPIC)\
        .option("startingOffsets", json.dumps(tpo))\
        .load()

    data = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")    

    windowSpec = \
        Window.partitionBy(df["topic"], df["partition"]).orderBy(df["offset"].desc())

    maxOffset = F.row_number().over(windowSpec)
    tpos = (df.withColumn("rowNumber", maxOffset)
        .where(F.col("rowNumber") == 1)
        .select("topic", "partition", "offset")
        .collect())

    print('*' * 50)
    update_topic_partition(tpos)

    print("data count: ", data.count())
    print('*' * 50)
    # data.write\
    #     .format("delta")\
    #     .mode("overwrite")\
    #     .save("s3a://hemnet-project/testHemnetbronzeNew")


def update_topic_partition(tpos):
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
        redis_client.hmset("hemnet:forsale:kafka", {topic: po_str})