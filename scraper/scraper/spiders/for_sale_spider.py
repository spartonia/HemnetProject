import scrapy

from urllib.parse import urlparse, urljoin, urlencode

from scrapy.exceptions import CloseSpider

from scraper.items import PageSourceItem
from scraper.kommun_ids import municipality_ids


BASE_URL = 'http://www.hemnet.se/bostader?'

# municipality_ids = [17797,]  # Umea
municipality_ids = municipality_ids
item_type_options = [
    ['fritidshus', 'tomt', 'gard', 'other'],
    ['villa'],
    ['bostadsratt'],
    ['radhus'],
]


LAST_VISITED_IDS = set('16952838')


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

    def __init__(self):
        super(ForSaleSpider, self).__init__()
        self.SHOULD_GO_NEXT_PAGE = True

    @classmethod
    def from_crawler(cls, crawler):
        spider = super(ForSaleSpider, cls).from_crawler(crawler)
        topic = crawler.settings.get('KAFKA_PRODUCER_TOPIC')
        if not topic:
            raise CloseSpider("'KAFKA_PRODUCER_TOPIC' is required.")
        brokers = crawler.settings.get('KAFKA_PRODUCER_BROKERS')
        if not topic:
            raise CloseSpider("'KAFKA_PRODUCER_BROKERS' is required.")
        return spider

    def start_requests(self):
        for url in start_urls():
            yield scrapy.Request(url, self.parse_list)

    def parse_list(self, response):
        urls = response.xpath('//*[@id="result"]/ul/li/a/@href').getall()

        for url in urls:
            d = url.split('-')[-1]
            if d in LAST_VISITED_IDS:
                self.SHOULD_GO_NEXT_PAGE = False
                continue
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
