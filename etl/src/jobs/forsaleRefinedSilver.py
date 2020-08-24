import json

import pyspark.sql.functions as F
from pyspark.sql.types import *


def analyze(spark, **kwargs):

    s3_source = kwargs.get('S3_SOURCE')
    if not s3_source:
        raise Exception("'S3_SOURCE' is required. You can pass it through '--job-args'")

    s3_sink = kwargs.get('S3_SINK')
    if not s3_sink:
        raise Exception("'S3_SINK' is required. You can pass it through '--job-args'")

    for_date = kwargs.get("FOR_DATE")
    if not for_date:
        raise Exception("'FOR_DATE' is required. You can pass it through '--job-args'")

    df = (spark
        .read
        .format("delta")
        .load(s3_source)
        .where(f"ingestion_date = '{for_date}'"))

    print("=" * 80)
    print(f"Number of records to be processed for date {for_date}: {df.count()}")
    print("=" * 80)

    value_schema = StructType([
        StructField('url', StringType(), True),
        StructField('source', StringType(), True),
        StructField('timestamp', DoubleType(), True)
    ])

    df = (df
        .withColumn("value_json", (F.from_json(F.col("value"), value_schema)))
        .withColumn("url", F.col("value_json.url"))
        .withColumn("source", F.col("value_json.source"))
        .withColumn("collected_on", F.col("value_json.timestamp"))
        .drop("value_json")
        .drop("timestampType")
        .drop("timestamp")
        .drop("key")
        .drop("value")
        .drop("topic")
        .drop("partition")
        .drop("offset")
    )

    # Get `ID` and `prop_type` from ur
    df = (df
        .withColumn('url_splitted', F.split(df.url, '/'))
        .withColumn('url_tail', F.element_at(F.col('url_splitted'), -1))
        .withColumn('tail_splitted', F.split(F.col('url_tail'), '-'))
        .withColumn('hemnet_id', F.element_at(F.col('tail_splitted'), -1))
        .withColumn('prop_type', F.element_at(F.col('tail_splitted'), 1))
        .drop('url_splitted')
        .drop('tail_splitted')
        .drop('url_tail')
    )

    # Get datalayer (props json)
    dataLayer_pattern = 'dataLayer\s*=\s*(\[.*\]);'
    df = (df
        .withColumn("data_layer", F.regexp_extract(F.col("source"), dataLayer_pattern, 1))
        .drop("source")
    )

    # Convert json string to struct
    rdds = df.rdd.map(lambda r: r.data_layer)
    jdf = spark.read.json(rdds)

    jdf = (jdf
        .where("property['id'] is not null")
        .selectExpr("property['id'] as prop_id", "property")
    )

    # jdf.printSchema()

    # join df and jsonified props
    joined = (df
        .join(jdf, df.hemnet_id == jdf.prop_id)
        .drop("data_layer")
        .drop("prop_id")
    )

    print(joined.columns)

    (joined
        .write
        .partitionBy("ingestion_date")
        .format("delta")
        .mode("append")
        .save(s3_sink))

    print('=' * 80)
    print(f'Stored {joined.count()} records into {s3_sink}')
    print('=' * 80)
