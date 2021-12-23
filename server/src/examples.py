from kafka_utils.base_consumer import BaseConsumer
from kafka_utils.base_producer import BaseProducer
import uuid
class MultiConsumer(BaseConsumer):
    topic = 'multi'
    group_id = uuid.uuid4()

    def on_data(self, data):
        print(data)
    
class MultiProducer(BaseProducer):
    topic = 'multi'

def main():
    MultiConsumer().listen()
    MultiProducer().send('Ultimate test message')

if __name__ == '__main__':
    main()