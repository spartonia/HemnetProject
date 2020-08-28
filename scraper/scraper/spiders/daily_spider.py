import scrapy
import xmltodict

from datetime import datetime
from dateutil import parser as date_parser

from twisted.internet.error import TimeoutError, TCPTimedOutError
from scrapy.exceptions import CloseSpider
from redis.connection import ConnectionError, ResponseError

from scraper.items import PageSourceItem



BASE_URL_SOLD = 'https://www.hemnet.se/sitemap/sold_properties.xml?page={}'
BASE_URL_FOR_SALE = 'https://www.hemnet.se/sitemap/items.xml?page={}'


class DailySpider(scrapy.Spider):
    """Spider for scraping items daily listed for sale or sold"""
    name = 'dailyspider'
    allowed_domains = ['hemnet.se']

    def __init__(self, *args, **kwargs):
        super(DailySpider, self).__init__()
        self.target = kwargs.get('target')
        self.fordate = date_parser.parse(kwargs.get('fordate'))
        self.SHOULD_GO_NEXT_PAGE = True

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        target = kwargs.get('target')
        if not target or target not in ['sold', 'forsale']:
           raise CloseSpider("'target' is required or invalid." +
            " Valid choice is either of: ['sold', 'forsale']")

        fordate = kwargs.get('fordate')
        if not fordate:
           raise CloseSpider("'fordate' is required or invalid.")

        topic = crawler.settings.get('KAFKA_PRODUCER_TOPIC')
        if not topic:
            raise CloseSpider("'KAFKA_PRODUCER_TOPIC' is required.")
        brokers = crawler.settings.get('KAFKA_PRODUCER_BROKERS')
        if not brokers:
            raise CloseSpider("'KAFKA_PRODUCER_BROKERS' is required.")

        return cls(crawler, *args, **kwargs)

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
            lastmod = date_parser.parse(el['lastmod'])
            url = el['loc']
            if lastmod.date() == self.fordate.date():
                yield scrapy.Request(url, self.get_page)

    def get_page(self, response):
        item = PageSourceItem()
        item['url'] = response.url
        item['source'] = response.text
        item['timestamp'] = datetime.utcnow().timestamp()

        yield item
