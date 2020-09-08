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
	-s KAFKA_PRODUCER_TOPIC=<kafka_topic_to_produce_results_to> \
	-s KAFKA_PRODUCER_BROKERS=broker:port,broker:port.. \
	-s REDIS_HOST=<redis-host> \
	[-s REDIS_PORT=<redis-port>]
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
	-s KAFKA_PRODUCER_TOPIC=<kafka_topic_to_produce_results_to> \
	-s KAFKA_PRODUCER_BROKERS=broker:port,broker:port.. \
	-s REDIS_HOST=<redis-host> \
	[-s REDIS_PORT=<redis-port>]
```

###### Airflow
Update the corresponding dag file in `./dags` folder and copy the
file to airflow dags folder.


### Historic data collection spiders
Spiders for collecting historical data

#### historicSoldURLCollector
Spider for collecting url of sold items. It collectec urls and puts into a redis set collection (`historicSoldURLCollector:sold_urls`). Ids of municipalities for wich their hostoric data is collectedis stored in redis at `historicSoldURLCollector:visited_municiplities` to avoid revisiting them.

__params__:
* `MAX_LOC_PER_RUN`: max number of municiplities to scrape per run (to avoid getting blocked)

##### How to Run
###### Bash
```bash
$ scrapy crawl historicSoldURLCollector \
	-s REDIS_HOST=localhost \
	-a MAX_LOC_PER_RUN=50
```

###### Docker
```bash
$ docker run  --net=host <TAG>[:<VERSION>] \
	historicSoldURLCollector \
	-s REDIS_HOST=<redis-host> \
	-a MAX_LOC_PER_RUN=<max-loc-per-run>

```

#### historicSoldSpider
Spider for downloading the sold urls collected by `historicSoldURLCollector`. It reads urls from redis (`historicSoldURLCollector:sold_urls`), downloads the page and publishes to `KAFKA_PRODUCER_TOPIC`, and marks them as downloaded at redis (`'hemnet:sold:collected_urls_bloom'`). It also extracts the canonical sale url and puts it into redis (`hemnet:forsale:canonical_urls`) for later processing.

__params__:
* `MAX_ITEMS_PER_RUN`: max number of urls to scrape per run (to avoid getting blocked) - Optional, default: `2400`

##### How to Run
###### Bash
```bash
$ scrapy crawl historicSoldSpider \
	[-a MAX_ITEMS_PER_RUN=<number>] \
	-s REDIS_HOST=<host> \
	-s KAFKA_PRODUCER_TOPIC=<topic> \
	-s KAFKA_PRODUCER_BROKERS=<host:port,host:port,..>
```

###### Docker
```bash
$ docker run  --net=host <TAG>[:<VERSION>] \
	historicSoldSpider \
	[-a MAX_ITEMS_PER_RUN=<number>] \
	-s REDIS_HOST=<host> \
	-s KAFKA_PRODUCER_TOPIC=<topic> \
	-s KAFKA_PRODUCER_BROKERS=<host:port,host:port,..>
```

#### historicCanonicalSpider
Spider for downloading canonical urls (expired forsale) after sols item. It reads urls from redis (`hemnet:forsale:canonical_urls`), downloads the page and publishes to `KAFKA_PRODUCER_TOPIC`, and marks them as downloaded at redis (`hemnet:forsale:collected_urls_bloom`).

__params__:
* `MAX_ITEMS_PER_RUN`: max number of urls to scrape per run (to avoid getting blocked) - Optional, default: `2400`

##### How to Run
###### Bash
```bash
$ scrapy crawl historicCanonicalSpider \
	[-a MAX_ITEMS_PER_RUN=<number>] \
	-s REDIS_HOST=<host> \
	-s KAFKA_PRODUCER_TOPIC=<topic> \
	-s KAFKA_PRODUCER_BROKERS=<host:port,host:port,..>
```

###### Docker
```bash
$ docker run  --net=host <TAG>[:<VERSION>] \
	historicCanonicalSpider \
	[-a MAX_ITEMS_PER_RUN=<number>] \
	-s REDIS_HOST=<host> \
	-s KAFKA_PRODUCER_TOPIC=<topic> \
	-s KAFKA_PRODUCER_BROKERS=<host:port,host:port,..>
```