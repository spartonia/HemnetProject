# Data Collection
___
## Scrapers

### Docker
Build image:
```bash
$ docker build . -t <TAG>[:<VERSION>]
```

### Items for sale
This spider collects items currently announced to be sold on Hemnet.se

#### How to run
###### Bash
On `scraper` folder,
```bash
$ scrapy crawl forsalespider \
	-s KAFKA_PRODUCER_TOPIC='some-topic' \
	-s KAFKA_PRODUCER_BROKERS=broker:port,broker:port.. \
	-s REDIS_HOST=localhost \
	[-s REDIS_PORT=port]

```

###### Docker
Build image and run:
```bash
$ docker run  --net=host <TAG>[:<VERSION>] forsalespider \
	-s KAFKA_PRODUCER_TOPIC='some-topic' \
	-s KAFKA_PRODUCER_BROKERS=broker:port,broker:port.. \
	-s REDIS_HOST=localhost \
	[-s REDIS_PORT=port]

```
Example:
```bash
$ docker run  --net=host hemnet-forsalesspider forsalespider \
	-s KAFKA_PRODUCER_TOPIC='test-topic' \
	-s KAFKA_PRODUCER_BROKERS=192.168.86.27:9092 \
	-s REDIS_HOST=192.168.86.27
```

###### Airflow
Update the corresponding dag file in `./dags` folder and copy the
file to airflow dags folder.


### Items sold (daily)
This spider collects daily items sold on Hemnet.se

#### How to run
###### Bash
On `scraper` folder,
```bash
$ scrapy crawl dailyspider \
	-a target='sold' \
	-a fordate='2020-08-22' \
	-s KAFKA_PRODUCER_TOPIC='some-topic' \
	-s KAFKA_PRODUCER_BROKERS=broker:port,broker:port.. 
```

###### Docker
cuild docker and run:
```bash
$ docker run --net=host <TAG>[:<VERSION>] dailyspider \
	-a target='sold' \
	-a fordate=2020-08-28 \
	-s KAFKA_PRODUCER_TOPIC='test-topic' \
	-s KAFKA_PRODUCER_BROKERS=localhost:9092,...
```