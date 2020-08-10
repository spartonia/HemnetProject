import scrapy

from urllib.parse import urlparse, urljoin, urlencode

from scrapy.exceptions import CloseSpider
from redisbloom.client import Client as BloomClient
from redis.connection import ConnectionError, ResponseError

from scraper.items import PageSourceItem
from scraper.kommun_ids import municipality_ids


BASE_URL = 'http://www.hemnet.se/bostader?'

# municipality_ids = [17797,]  # Umea
# TODO
municipality_ids = [17865,]  # Alle
# municipality_ids = municipality_ids
# TODO
item_type_options = [
    # ['fritidshus', 'tomt', 'gard', 'other'],
    ['villa'],
    # ['bostadsratt'],
    # ['radhus'],
]


def start_urls():
    for loc in municipality_ids:
        for item_types in item_type_options:
            p = {
                'municipality_ids[]': loc,
                'item_types[]': item_types
            }
            yield BASE_URL + urlencode(p, True)


class ForSaleSpider(scrapy.Spider):
    """Spider for scraping items currently listed for sale"""
    name = 'forsalespider'
    allowed_domains = ['hemnet.se']

    def __init__(self, redis_host, redis_port):
        super(ForSaleSpider, self).__init__()
        self.SHOULD_GO_NEXT_PAGE = True
        self.redis = self._connect_to_redis(redis_host, redis_port)
        self._maybe_setup_bloom()

    def _connect_to_redis(self, host, port):
        return BloomClient(host=host, port=port)

    def _maybe_setup_bloom(self):
        try:
            self.redis.bfCreate(self.name, 0.01, 1_000_000)
        except ResponseError:
            pass
        except Exception as e:
            raise CloseSpider(e)

    @classmethod
    def from_crawler(cls, crawler):
        # spider = super(ForSaleSpider, cls).from_crawler(crawler)
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
            redis_port=crawler.settings.get('REDIS_PORT', 6379)
        )

    def _is_url_visited(self, url):
        id = url.split('-')[-1]
        return self.redis.bfExists(self.name, id)

    def _mark_as_visited(self, url):
        id = url.split('-')[-1]
        return self.redis.bfAdd(self.name, id)

    def start_requests(self):
        for url in start_urls():
            yield scrapy.Request(url, self.parse_list)

    def parse_list(self, response):
        urls = response.xpath('//*[@id="result"]/ul/li/a/@href').getall()

        for url in urls:
            if self._is_url_visited(url):
                print('Already visited, skipping: ', url)
                self.SHOULD_GO_NEXT_PAGE = False
                continue
            else:
                self._mark_as_visited(url)
                yield scrapy.Request(url, self.parse)

        next_href = response.css('a.next_page::attr("href")').extract_first()

        if next_href and self.SHOULD_GO_NEXT_PAGE:
            next_url = urljoin(response.url, next_href)
            scrapy.Request(next_url, self.parse_list)


    def parse(self, response):
        item = PageSourceItem()
        item['url'] = response.url
        item['source'] = response.text

        yield item
