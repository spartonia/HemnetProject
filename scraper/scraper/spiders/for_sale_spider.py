import scrapy
from scrapy.exceptions import CloseSpider

from scraper.items import PageSourceItem


def start_urls():
    return [
        'https://www.hemnet.se/bostad/villa-7rum-tvaaker-varbergs-kommun-hedvagen-32-16959086',
        'https://www.hemnet.se/bostad/fritidsboende-4rum-haggsjo-harnosands-kommun-nynas-104-16959117',
    ]


class ForSaleSpider(scrapy.Spider):
    """Spider for scraping items currently listed for sale"""
    name = 'forsalespider'

    def __init__(self):
        super(ForSaleSpider, self).__init__()

    @classmethod
    def from_crawler(cls, crawler):
        topic = crawler.settings.get('KAFKA_PRODUCER_TOPIC')
        if not topic:
            raise CloseSpider("'KAFKA_PRODUCER_TOPIC' is required.")
        brokers = crawler.settings.get('KAFKA_PRODUCER_BROKERS')
        if not topic:
            raise CloseSpider("'KAFKA_PRODUCER_BROKERS' is required.")
        return cls()

    def start_requests(self):
        for url in start_urls():
            yield scrapy.Request(url, self.parse)

    def parse(self, response):
        item = PageSourceItem()
        item['url'] = response.url
        item['source'] = response.body_as_unicode()

        yield item


