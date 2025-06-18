from kafka import KafkaProducer
import json
import time
import threading
from datetime import datetime
from typing import List, Dict, Any
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BatchProducer:
    def __init__(self, bootstrap_servers=['localhost:9092'], batch_size=100, linger_ms=1000):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8') if k else None,
            batch_size=16384,  # Batch size in bytes
            linger_ms=linger_ms,  # Wait time before sending batch
            acks='all',  # Wait for all replicas
            retries=3,
            compression_type='gzip'
        )
        self.batch_size = batch_size
        self.message_buffer = []
        self.buffer_lock = threading.Lock()
        
    def add_message(self, topic: str, message: Dict[Any, Any], key: str = None):
        """Add message to batch buffer"""
        with self.buffer_lock:
            self.message_buffer.append({
                'topic': topic,
                'message': message,
                'key': key,
                'timestamp': datetime.now().isoformat()
            })
            
            # Send batch if buffer is full
            if len(self.message_buffer) >= self.batch_size:
                self._send_batch()
    
    def _send_batch(self):
        """Send all messages in buffer"""
        if not self.message_buffer:
            return
            
        logger.info(f"Sending batch of {len(self.message_buffer)} messages")
        
        for msg_data in self.message_buffer:
            try:
                future = self.producer.send(
                    msg_data['topic'],
                    value=msg_data['message'],
                    key=msg_data['key']
                )
                # Don't wait for each message - let batching handle it
            except Exception as e:
                logger.error(f"Error sending message: {e}")
        
        self.message_buffer.clear()
        
    def flush_batch(self):
        """Manually flush remaining messages"""
        with self.buffer_lock:
            if self.message_buffer:
                self._send_batch()
        self.producer.flush()
    
    def simulate_high_volume_data(self, topic: str, count: int = 1000):
        """Simulate high-volume data ingestion"""
        logger.info(f"Simulating {count} messages to topic '{topic}'")
        
        start_time = time.time()
        
        for i in range(count):
            message = {
                'id': i,
                'user_id': f"user_{i % 100}",  # 100 different users
                'event_type': ['login', 'purchase', 'view', 'logout'][i % 4],
                'timestamp': datetime.now().isoformat(),
                'metadata': {
                    'session_id': f"session_{i // 10}",
                    'ip_address': f"192.168.1.{i % 255}",
                    'user_agent': 'BatchProducer/1.0'
                }
            }
            
            # Use user_id as key for partitioning
            self.add_message(topic, message, key=f"user_{i % 100}")
            
            # Small delay to simulate realistic timing
            if i % 100 == 0:
                time.sleep(0.01)
        
        # Flush remaining messages
        self.flush_batch()
        
        end_time = time.time()
        duration = end_time - start_time
        throughput = count / duration
        
        logger.info(f"Sent {count} messages in {duration:.2f}s ({throughput:.2f} msgs/sec)")
    
    def close(self):
        self.flush_batch()
        self.producer.close()

if __name__ == "__main__":
    batch_producer = BatchProducer(batch_size=50, linger_ms=500)
    
    try:
        # Simulate different types of batch processing
        batch_producer.simulate_high_volume_data('batch-processing', 500)
        
        # Add some individual messages
        for i in range(10):
            message = {
                'type': 'individual',
                'id': i,
                'content': f'Individual message {i}'
            }
            batch_producer.add_message('batch-processing', message, f'individual-{i}')
        
    finally:
        batch_producer.close()