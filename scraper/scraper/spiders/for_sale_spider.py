import scrapy

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

    def start_requests(self):
        for url in start_urls():
            yield scrapy.Request(url, self.parse)

    def parse(self, response):
        item = PageSourceItem()
        item['url'] = response.url
        item['source'] = response.body_as_unicode()

        yield item


