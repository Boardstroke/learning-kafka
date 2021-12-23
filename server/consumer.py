from confluent_kafka import Consumer
from abc import ABC, abstractmethod, abstractproperty
import sys
import threading

class BaseConsumer(ABC):
    
    @abstractproperty
    def topic(self):
        pass

    @abstractproperty
    def group_id(self):
        pass


    @abstractmethod
    def consume(self, msg):
        pass

    def __init__(self):
        self.config = {
            'bootstrap.servers': 'localhost:9093',
            'group.id': self.group_id,
            'auto.offset.reset': 'smallest',
        }
        self.running = True
    
    def listen(self):
        thread = threading.Thread(target=self.run)
        thread.start()

        return thread
    
    def run(self):
        print("Starting consumer... {}".format(self.__class__.__name__))
        consumer = Consumer(self.config)
        try:
            consumer.subscribe([self.topic])
            while self.running:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    print("Consumer error: {}".format(msg.error()))
                    continue
                print('Received message: {}; Group id: {}'.format(msg.value().decode('utf-8'), self.group_id))
                self.consume(msg.value().decode('utf-8') )
           

        except KeyboardInterrupt:
            print("\n")
            print("Exiting...")
            sys.exit(1)
        finally:
            consumer.close()
        
    def shutdown(self):
        print("Shutting down consumer... {}".format(self.__class__.__name__))
        self.running = False

class QuickstartEventsConsumer(BaseConsumer):
    group_id = 'multi'
    topic = 'multi'

    def consume(self, msg):
        print('Received message: {}'.format(msg))

class OtherConsumer(BaseConsumer):
    group_id = 'other-consumer'
    topic = 'multi'

    def consume(self, msg):
        print('Received message other consumer: {}'.format(msg))


