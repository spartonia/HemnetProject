import re
import scrapy

from datetime import datetime
from urllib.parse import urlparse, urljoin, urlencode

from scrapy import signals
from pydispatch import dispatcher
from twisted.internet.error import TimeoutError, TCPTimedOutError
from scrapy.exceptions import CloseSpider
from redisbloom.client import Client as BloomClient
from redis.connection import ConnectionError, ResponseError

from scraper.items import PageSourceItem
from scraper.kommun_ids import municipality_ids


BASE_URL = 'http://www.hemnet.se/salda/bostader?'

# municipality_ids = [17865, 898740]  # Alle
municipality_ids = municipality_ids

fat_municipalities = ['17800', '924030', '898748', '474368', '18028']


def start_urls(sold_age, loc):
    p = {
        'municipality_ids[]': loc,
        'sold_age': sold_age,
    }

    if loc in fat_municipalities:
        return [
            BASE_URL + urlencode({**p, **{'rooms_max': 2}}),
            BASE_URL + urlencode({**p, **{'rooms_min': 2.5}}),
        ]
    else:
        return [BASE_URL + urlencode(p, True)]


class HistoricSoldURLCollector(scrapy.Spider):
    """Spider for scraping urls of sold items (historical) """

    name = 'historicSoldURLCollector'
    allowed_domains = ['hemnet.se']

    def __init__(self, redis_host, redis_port, *args, **kwargs):
        super(HistoricSoldURLCollector, self).__init__()
        self.sold_age = kwargs.get('SOLD_AGE', '12m')
        self.max_locations_per_run = int(kwargs.get('MAX_LOC_PER_RUN', 5))
        self.redis = self._connect_to_redis(redis_host, redis_port)
        dispatcher.connect(self.spider_closed, signals.spider_closed)

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        redis_host = crawler.settings.get('REDIS_HOST')
        if not redis_host:
            raise CloseSpider("'REDIS_HOST' is required.")
        
        return cls(
            redis_host=redis_host,
            redis_port=crawler.settings.get('REDIS_PORT', 6379),
            *args, **kwargs,
        )

    @property
    def visited_municipalities_redis(self):
        return f"{self.name}:visited_municiplities"
    
    @property
    def sold_urls_redis(self):
        return f"{self.name}:sold_urls"

    def _connect_to_redis(self, host, port):
        return BloomClient(host=host, port=port)

    def _mark_loc_as_visited(self, loc):
        self.redis.sadd(self.visited_municipalities_redis, loc)

    def _get_next_municipalities(self):
        redis_visited = f"{self.name}:visited_municiplity"
        locs = []
        for loc in municipality_ids:
            if self.redis.sismember(self.visited_municipalities_redis, loc):
                continue
            else:
                locs.append(loc)
            if len(locs) >= self.max_locations_per_run:
                return locs
        return locs


    def start_requests(self):
        locs = self._get_next_municipalities()
        if not locs:
            print("=" * 10 + "All municipality ids are visited.")
        else:
            for loc in locs:
                for url in start_urls(self.sold_age, loc):
                    self._mark_loc_as_visited(loc)
                    yield scrapy.Request(url, self.parse_list)

    def parse_list(self, response):
        url_pattern = 'href="(https\:.*\/salda\/.*[0-9])">'
        urls = re.findall(url_pattern, response.text)

        if urls:
            self.redis.sadd(self.sold_urls_redis, *urls)

        next_href = response.css('a.next_page::attr("href")').extract_first()

        if next_href:
            next_url = urljoin(response.url, next_href)
            yield scrapy.Request(next_url, self.parse_list)

    def spider_closed(self, spider):
        pass


"""
NOTE: You can use order by desc to get the old results first
"""