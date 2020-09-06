import json

from delta.tables import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql import functions as F


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
        .withColumn('sold_id', F.element_at(F.col('tail_splitted'), -1))
        .withColumn('prop_type', F.element_at(F.col('tail_splitted'), 1))
        .drop('url_splitted')
        .drop('tail_splitted')
        .drop('url_tail')
    )

    # Get datalayer (props json)
    # datalayer_pattern = 'dataLayer\s*=\s*(\[.*\]);'
    property_pattern = 'dataLayer\s*=\s*\[.*\"property\":\s*(\{.*)\}\];'
    sold_property_pattern = 'dataLayer\s*=\s*\[.*\"sold_property\":\s*(\{.*)\}\];'
    df = (df
        .withColumn("prop_str", F.regexp_extract(F.col("source"), property_pattern, 1))
        .withColumn("sold_prop_str", F.regexp_extract(F.col("source"), sold_property_pattern, 1))
        # .withColumn("dlayer_str", F.regexp_extract(F.col("source"), datalayer_pattern, 1))
        .drop("source")
    )

    property_schema = StructType().add('id', IntegerType(), True)

    sold_property_schema = (StructType()
      .add("id", LongType(), True)
      .add("broker_agency", StringType(), True)
      .add("broker_agency_id", IntegerType(), True)
      .add("location", StringType(), True)
      .add("locations", StructType()
        .add("country", StringType(), True)
        .add("county", StringType(), True)
        .add("municipality", StringType(), True)
        .add("postal_city", StringType(), True)
        .add("street", StringType(), True)
        .add("city", StringType(), True)
        .add("district", StringType(), True))
      .add("street_address", StringType(), True)
      .add("price", LongType(), True)
      .add("selling_price", LongType(), True)
      .add("rooms", StringType(), True)
      .add("living_area", StringType(), True)
      .add("sold_at_date", StringType(), True)
    )

    df = (df
        .withColumn('prop', F.from_json(df.prop_str, property_schema))
        .withColumn('hemnet_id', F.col('prop.id'))
        .drop('prop')
        .drop('prop_str')
        .withColumn('sold_props', F.from_json(df.sold_prop_str, sold_property_schema))
        .withColumn('sold_id', F.col('sold_props.id'))
        .where("sold_id is not null")
        .drop("sold_prop_str")
    )

    windowSpec = (Window
        .partitionBy('sold_id')
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
