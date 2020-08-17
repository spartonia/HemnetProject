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

val df = spark.read.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "test-topic").option("startingOffsets", \"""{"test-topic": {"0": 23}}\""").load()


val windowSpec = Window.partitionBy(df("topic"), df("partition")).orderBy($"offset".desc)
val maxOffset =  row_number.over(windowSpec)
df.withColumn("rowNumber", maxOffset).where($"rowNumber" === 1).select("topic", "partition", "offset").show(5)
"""
import redis


def analyze(spark):

    df = spark\
        .read\
        .format("kafka")\
        .option("kafka.bootstrap.servers", "localhost:9092")\
        .option("subscribe", "test-topic")\
        .load()

    data = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")    

    print('*' * 50)
    data.show(5)
    print('*' * 50)
    data.write\
        .format("delta")\
        .mode("overwrite")\
        .save("s3a://hemnet-project/testHemnetbronzeNew")
