import json

from delta.tables import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql import functions as F

from shared.schema import canonical_schema as json_schema


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

    print("*" * 20,
        f"Number of records to be processed for date {for_date}: {df.count()}",
        "*" * 20)

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
    property_pattern = 'dataLayer\s*=\s*\[.*\"property\":\s*(\{.*)\}\];'
    df = (df
        .withColumn("props_str", F.regexp_extract(F.col("source"), property_pattern, 1))
        .drop("source")
    )

    df = (df
        .withColumn('props', F.from_json(df.props_str, json_schema))
        .where("props['id'] is not null")
        .drop("props_str")
    )

    windowSpec = (Window
        .partitionBy('hemnet_id')
        .orderBy(F.desc('ingestion_date')))

    df = (df
        .withColumn('rank', F.row_number().over(windowSpec))
        .where('rank == 1')
        .drop('rank')
      )
  
    exists = DeltaTable.isDeltaTable(spark, s3_sink)

    if not exists:
        (df
            .write
            .partitionBy("ingestion_date")
            .format("delta")
            .mode("append")
            .save(s3_sink))
    else:
        deltaTable = DeltaTable.forPath(spark, s3_sink)
        (deltaTable
            .alias("t")
            .merge(df.alias("s"), "t.hemnet_id = s.hemnet_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())

    print('*' * 20, f'Stored {df.count()} records into {s3_sink}', '*' * 20)
