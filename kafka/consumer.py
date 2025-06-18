from kafka import KafkaConsumer
import json
import signal
import sys

class SimpleConsumer:
    def __init__(self, topics, bootstrap_servers=['localhost:9092'], group_id='test-group'):
        self.consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='earliest',  # Start from beginning if no offset
            enable_auto_commit=True
        )
        self.running = True
        
        # Handle graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        print("\nShutting down consumer...")
        self.running = False
    
    def consume_messages(self):
        print("Starting consumer... Press Ctrl+C to stop")
        try:
            for message in self.consumer:
                if not self.running:
                    break
                
                print(f"Received message:")
                print(f"  Topic: {message.topic}")
                print(f"  Partition: {message.partition}")
                print(f"  Offset: {message.offset}")
                print(f"  Key: {message.key}")
                print(f"  Value: {message.value}")
                print(f"  Timestamp: {message.timestamp}")
                print("-" * 50)
                
        except Exception as e:
            print(f"Error consuming messages: {e}")
        finally:
            self.close()
    
    def close(self):
        self.consumer.close()
        print("Consumer closed")

if __name__ == "__main__":
    consumer = SimpleConsumer(['test-topic'])
    consumer.consume_messages()