import time
import scrapy
import xmltodict

from datetime import datetime
from dateutil import parser as date_parser

from scrapy.exceptions import CloseSpider
from redisbloom.client import Client as BloomClient
from redis.connection import ConnectionError, ResponseError
from twisted.internet.error import TimeoutError, TCPTimedOutError

from scraper.items import PageSourceItem



BASE_URL_SOLD = 'https://www.hemnet.se/sitemap/sold_properties.xml?page={}'
BASE_URL_FOR_SALE = 'https://www.hemnet.se/sitemap/items.xml?page={}'


class DailySpider(scrapy.Spider):
    """Spider for scraping items daily listed for sale or sold"""
    name = 'dailyspider'
    allowed_domains = ['hemnet.se']

    def __init__(self, redis_host, redis_port, *args, **kwargs):
        super().__init__()
        self.target = kwargs.get('target')
        self.redis = self._connect_to_redis(redis_host, redis_port)
        self._maybe_setup_bloom()

    @property
    def bloom_name(self):
        return f'hemnet:{self.target}:collected_urls_bloom'

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        target = kwargs.get('target')
        if not target or target not in ['sold', 'forsale']:
           raise CloseSpider("'target' is required or invalid." +
            " Valid choice is either of: ['sold', 'forsale']")

        topic = crawler.settings.get('KAFKA_PRODUCER_TOPIC')
        if not topic:
            raise CloseSpider("'KAFKA_PRODUCER_TOPIC' is required.")
        brokers = crawler.settings.get('KAFKA_PRODUCER_BROKERS')
        if not brokers:
            raise CloseSpider("'KAFKA_PRODUCER_BROKERS' is required.")

        redis_host = crawler.settings.get('REDIS_HOST')
        if not redis_host:
            raise CloseSpider("'REDIS_HOST' is required.")

        return cls(
            redis_host=redis_host,
            redis_port=crawler.settings.get('REDIS_PORT', 6379),
            *args, **kwargs)

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

    def _mark_as_visited(self, url):
        id = url.split('-')[-1]
        return self.redis.bfAdd(self.bloom_name, id)

    def start_requests(self):
        if self.target == 'sold':
            BASE_URL = BASE_URL_SOLD
        else:
            BASE_URL = BASE_URL_FOR_SALE
        for i in range(1, 26):
            url = BASE_URL.format(i)
            yield scrapy.Request(url, self.parse_xml)


    def parse_xml(self, response):
        parsed = xmltodict.parse(response.text)
        rs = parsed['urlset']['url']
        for el in rs:
            url = el['loc']
            if self._is_url_visited(url):
                print('Already visited, skipping: ', url)
            else:
                yield scrapy.Request(url, self.get_page)

    def get_page(self, response):
        self._mark_as_visited(response.url)

        item = PageSourceItem()
        item['url'] = response.url
        item['source'] = response.text
        item['timestamp'] = datetime.utcnow().timestamp()

        yield item
