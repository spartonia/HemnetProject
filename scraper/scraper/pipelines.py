# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
# from .settings import KAFKA_SETTINGS
from itemadapter import ItemAdapter
from scrapy.utils.serialize import ScrapyJSONEncoder

from confluent_kafka import Producer


class KafkaPipeline:
    def __init__(self, producer, topic):
        self.p = producer
        self.topic = topic
        self.encoder = ScrapyJSONEncoder()

    @classmethod
    def from_settings(cls, settings):
        """
        :param settings: the current Scrapy settings
        :type settings: scrapy.settings.Settings
        :rtype: A :class:`~KafkaPipeline` instance
        """
        brokers = settings.get('KAFKA_PRODUCER_BROKERS')
        topic = settings.get('KAFKA_PRODUCER_TOPIC')
        producer = Producer({
            'bootstrap.servers': brokers,
        })
        return cls(producer, topic)

    def process_item(self, item, spider):
        msg = self.encoder.encode(item)
        self.p.produce(self.topic, msg, callback=delivery_report)


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
