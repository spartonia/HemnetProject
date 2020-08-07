# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
# import settings
from itemadapter import ItemAdapter
from scrapy.utils.serialize import ScrapyJSONEncoder

from confluent_kafka import Producer


class KafkaPipeline:
    def __init__(self):
        self.p = Producer({
            'bootstrap.servers': 'localhost:9092',
        })
        self.encoder = ScrapyJSONEncoder()

    def process_item(self, item, spider):
        topic = 'csptest'
        msg = self.encoder.encode(item)
        self.p.produce(topic, msg, callback=delivery_report)


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
