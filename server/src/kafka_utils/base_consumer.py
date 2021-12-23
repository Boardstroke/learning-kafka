from multiprocessing import Process
from confluent_kafka import Consumer
from abc import ABC, abstractmethod, abstractproperty
import sys
import ast
import logging

from functools import wraps

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('consumer')

def multiprocess(fn):
    @wraps(fn)
    def call(*args, **kwargs):
        p = Process(target=fn, args=args, kwargs=kwargs)
        p.start()
        return p
    return call


class BaseConsumer(ABC):
    
    @abstractproperty
    def topic(self):
        pass

    @abstractproperty
    def group_id(self):
        pass


    @abstractmethod
    def on_data(self, data):
        pass

    def __init__(self):
        self.config = {
            'bootstrap.servers': 'localhost:9093',
            'group.id': self.group_id,
            'auto.offset.reset': 'smallest',
        }
        self.running = True
    
    
    @multiprocess
    def listen(self):
        logger.info("Starting consumer... {}".format(self.__class__.__name__))
        consumer = Consumer(self.config)
        try:
            consumer.subscribe([self.topic])
            while self.running:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    logger.error("Consumer error: {}".format(msg.error()))
                    continue
                logger.info('Received message: {}; Group id: {}'.format(msg.value().decode('utf-8'), self.group_id))
                self.on_data(self.parse_data(msg.value().decode('utf-8')) )

            consumer.close()

        except KeyboardInterrupt:
            logger.info("Exiting...")
            sys.exit(1)
        finally:
            consumer.close()

    def parse_data(self, data):
        try:
            return ast.literal_eval(data)
        except Exception as e:
            logger.error("Error: {}".format(e))
        finally: 
            return data

    def shutdown(self):
        self.running = False
        
    

    


