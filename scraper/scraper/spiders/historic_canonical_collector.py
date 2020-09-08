import scrapy

from datetime import datetime

from scrapy.exceptions import CloseSpider
from redisbloom.client import Client as BloomClient
from redis.connection import ConnectionError, ResponseError

from scraper.items import PageSourceItem


class HistoricCanonicalSpider(scrapy.Spider):
    """Spider for scraping canonical forsale data for sold items (historical).
    URLs read from redis. 
    """

    name = 'historicCanonicalSpider'
    allowed_domains = ['hemnet.se']

    def __init__(self, redis_host, redis_port, *args, **kwargs):
        super().__init__()
        self.max_items_per_run = int(kwargs.get('MAX_ITEMS_PER_RUN', 2400))
        self.redis = self._connect_to_redis(redis_host, redis_port)

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        redis_host = crawler.settings.get('REDIS_HOST')
        if not redis_host:
            raise CloseSpider("'REDIS_HOST' is required.")

        topic = crawler.settings.get('KAFKA_PRODUCER_TOPIC')
        if not topic:
            raise CloseSpider("'KAFKA_PRODUCER_TOPIC' is required.")
        brokers = crawler.settings.get('KAFKA_PRODUCER_BROKERS')
        if not brokers:
            raise CloseSpider("'KAFKA_PRODUCER_BROKERS' is required.")
        
        return cls(
            redis_host=redis_host,
            redis_port=crawler.settings.get('REDIS_PORT', 6379),
            *args, **kwargs,
        )
    

    @property
    def canonical_urls_redis(self):
        return "hemnet:forsale:canonical_urls"

    @property
    def bloom_name(self):
        return 'hemnet:forsale:collected_urls_bloom'
    

    def _connect_to_redis(self, host, port):
        return BloomClient(host=host, port=port)

    def _maybe_setup_bloom(self):
        try:
            self.redis.bfCreate(self.bloom_name, 0.001, 1_000_000)
        except ResponseError:
            pass
        except Exception as e:
            raise CloseSpider(e)

    def _is_url_visited(self, url):
        id = url.split('-')[-1]
        return self.redis.bfExists(self.bloom_name, id)

    def _get_urls_from_redis(self):
        return self.redis.srandmember(self.canonical_urls_redis,
            self.max_items_per_run)

    def _mark_url_as_visited(self, url):
        self.redis.srem(self.canonical_urls_redis, url)
        id = url.split('-')[-1]
        self.redis.bfAdd(self.bloom_name, id)

    def start_requests(self):
        urls = self._get_urls_from_redis()
        print("=" * 20 + f" Num urls to scrape: {len(urls)}")

        for url in urls:
            url_decoded = url.decode('utf-8')
            if self._is_url_visited(url_decoded):
                print('Already visited, skipping: ', url_decoded)
                self.redis.srem(self.canonical_urls_redis, url_decoded)
            else:
                yield scrapy.Request(url_decoded, self.download_page)


    def download_page(self, response):
        if response.status < 300:
            item = PageSourceItem()
            item['url'] = response.url
            item['source'] = response.text
            item['timestamp'] = datetime.utcnow().timestamp()

            self._mark_url_as_visited(response.url)
            yield item
