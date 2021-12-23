from abc import ABC, abstractproperty
import logging
from confluent_kafka import Producer

class BaseProducer(ABC):
    @abstractproperty
    def topic(self):
        pass


    def __init__(self):
        self.config = {
            'bootstrap.servers': 'localhost:9093',
            'client.id': 'producer',
        }
    
    def send(self, data):
        producer = Producer(self.config)
        producer.produce(self.topic, data)
        producer.flush()

