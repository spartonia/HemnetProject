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
1. `cd` to folder `etl`.

2. Install/update requirements into a folder:
```bash
$ pip install -r requirements.txt -t ./src/libs
```

3. Package python files:
```bash
$ make build
```

4. Then `cd` to `dist` and run:
```bash
spark-submit \
	--packages io.delta:delta-core_2.12:0.7.0,org.apache.hadoop:hadoop-aws:2.7.7,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0  \
	--conf spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore  \
	--conf spark.hadoop.fs.s3a.endpoint=s3-eu-north-1.amazonaws.com  \
	--conf spark.driver.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true  \
	--conf spark.executor.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true  \
	--conf spark.hadoop.fs.s3a.access.key=$AWS_S3_ACCESS  \
	--conf spark.hadoop.fs.s3a.secret.key=$AWS_S3_SECRET  \
	jobs/forsaleKafkaToBronze.py 
```
__Note__: If you have changed the code base, run `make build` again before submitting the new code.

## Internals
* Consumed kafka `topicParitions` (`topic + partition + offset`) are stored in redis as json string in the following manner:
	- For each topic, partitions and offsets are jsonified as `val = json.dumps({"partition#": offset, ...})`
	- Then values are stored in a redis hash with this schema:
    	```bash
    		hemnet:<spider>:kafka <kafka-topic> <val>
    	```
	- So, for `forsale` spider, in redis-cli:
    	```
    		HGET hemnet:forsale:kafka test-topic
    	```
	    will return something similar to `"{\"0\": 26, \"1\": 34}"`
