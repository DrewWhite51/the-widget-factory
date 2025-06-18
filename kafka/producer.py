from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

class SimpleProducer:
    def __init__(self, bootstrap_servers=['localhost:9092']):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8') if k else None
        )
    
    def send_message(self, topic, message, key=None):
        try:
            future = self.producer.send(topic, value=message, key=key)
            record_metadata = future.get(timeout=10)
            print(f"Message sent to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")
            return True
        except Exception as e:
            print(f"Error sending message: {e}")
            return False
    
    def send_batch_messages(self, topic, count=10):
        print(f"Sending {count} messages to topic '{topic}'...")
        for i in range(count):
            message = {
                'id': i,
                'timestamp': datetime.now().isoformat(),
                'message': f"Hello Kafka! Message #{i}",
                'random_value': random.randint(1, 100)
            }
            self.send_message(topic, message, key=f"key-{i}")
            time.sleep(0.5)  # Small delay between messages
    
    def close(self):
        self.producer.close()

if __name__ == "__main__":
    producer = SimpleProducer()
    
    try:
        # Send a single message
        single_message = {
            'type': 'greeting',
            'content': 'Hello from Kafka Producer!',
            'timestamp': datetime.now().isoformat()
        }
        producer.send_message('test-topic', single_message, 'greeting-key')
        
        # Send batch messages
        producer.send_batch_messages('test-topic', 5)
        
    finally:
        producer.close()