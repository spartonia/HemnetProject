## SPARK shell Delta Lake on AWS :
Set AWS S3 access and secrets:
```bash
$ export AWS_S3_ACCESS=<AWS_S3_ACCESS>
$ export AWS_S3_SECRET=<AWS_S3_SECRET>
```

__spark-shell__:
```bash
spark-shell  \
	--packages io.delta:delta-core_2.12:0.7.0,org.apache.hadoop:hadoop-aws:2.7.7,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0  \
	--conf spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore  \
	--conf spark.hadoop.fs.s3a.endpoint=s3-eu-north-1.amazonaws.com \
	--conf spark.driver.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true \
	--conf spark.executor.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true \
	--conf spark.hadoop.fs.s3a.access.key=$AWS_S3_ACCESS  \
	--conf spark.hadoop.fs.s3a.secret.key=$AWS_S3_SECRET
```

__spark-submit__:
```bash
spark-submit \
	--packages io.delta:delta-core_2.12:0.7.0,org.apache.hadoop:hadoop-aws:2.7.7,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0  \
	--conf spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore  \
	--conf spark.hadoop.fs.s3a.endpoint=s3-eu-north-1.amazonaws.com  \
	--conf spark.driver.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true  \
	--conf spark.executor.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true  \
	--conf spark.hadoop.fs.s3a.access.key=$AWS_S3_ACCESS  \
	--conf spark.hadoop.fs.s3a.secret.key=$AWS_S3_SECRET  \
	etl/jobs/forsaleKafkaToBronze.py 
```
