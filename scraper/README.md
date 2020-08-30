# Data Collection
___
## Scrapers

### Daily data collection Spiders
Spiders for collecting items (forsale/sold) on daily basis from Hemnet.se.
These Spiders do not collect historical data

#### How to run
__Note__:
* `target` is one of `forsale` or `sold`.
* `fordate` is string date of `YYYY-MM-DD` format.

###### Bash
On `scraper` folder, run:
```bash
$ scrapy crawl dailyspider \
	-a target=<target> \
	-a fordate=<fordate> \
	-s KAFKA_PRODUCER_TOPIC=<kafka_topic_to_produce_results_to> \
	-s KAFKA_PRODUCER_BROKERS=broker:port,broker:port.. 
```

###### Docker
Build image:
```bash
$ docker build . -t <TAG>[:<VERSION>]
```

__Hint__: Tag with latest git hash:
```bash
$ docker build . -t <TAG>$(git log -1 --format=%h)
```

Run docker:
```bash
$ docker run --net=host <TAG>[:<VERSION>] dailyspider \
	-a target=<target> \
	-a fordate=<fordate> \
	-s KAFKA_PRODUCER_TOPIC=<kafka_topic_to_produce_results_to> \
	-s KAFKA_PRODUCER_BROKERS=broker:port,broker:port..
```

###### Airflow
Update the corresponding dag file in `./dags` folder and copy the
file to airflow dags folder.
