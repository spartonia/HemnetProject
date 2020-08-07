# Data Collection
___
## Scrapers
### Items for sale
This spider collects items currently announced to be sold on Hemnet.se

#### How to run
On `scraper` folder,
```bash
$ scrapy crawl forsalespider \
	-s KAFKA_PRODUCER_TOPIC='some-topic' \
	-s KAFKA_PRODUCER_BROKERS=broker:port,broker:port..

```