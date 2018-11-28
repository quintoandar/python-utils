import json
import os

from kafka import KafkaProducer


class KafkaDispatcher:
    def __init__(self, kafka_producer=None, configs=None):
        if configs is None:
            configs = {
                'bootstrap_servers':
                os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
                'value_serializer':
                self.value_serializer,
                'key_serializer':
                self.key_serializer
            }

        if kafka_producer is None:
            self.kafka_producer = KafkaProducer(**configs)
        else:
            self.kafka_producer = kafka_producer

    @staticmethod
    def value_serializer(v):
        return json.dumps(v).encode('utf-8')

    @staticmethod
    def key_serializer(k):
        return k.encode('utf-8') if k is not None else k

    def dispatch_message(self,
                         topic,
                         event_name,
                         payload,
                         source,
                         timeout=None):
        kafka_json = {
            "topic": topic,
            "eventName": event_name,
            "source": source,
            "payload": payload
        }
        self.kafka_producer.send(topic, kafka_json)
        self.kafka_producer.flush(timeout)
