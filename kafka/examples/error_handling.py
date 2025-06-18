import json
import time
import logging
from datetime import datetime
from typing import Optional, Dict, Any
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError, KafkaTimeoutError, CommitFailedError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RobustProducer:
    def __init__(self, bootstrap_servers=['localhost:9092'], max_retries=3):
        self.max_retries = max_retries
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8') if k else None,
            acks='all',  # Ensure all replicas acknowledge
            retries=max_retries,
            retry_backoff_ms=1000,
            request_timeout_ms=30000,
            max_block_ms=60000
        )
        self.dlq_producer = None  # Dead Letter Queue producer
        self._init_dlq_producer()
    
    def _init_dlq_producer(self):
        """Initialize Dead Letter Queue producer"""
        try:
            self.dlq_producer = KafkaProducer(
                bootstrap_servers=self.producer.config['bootstrap_servers'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: str(k).encode('utf-8') if k else None
            )
        except Exception as e:
            logger.error(f"Failed to initialize DLQ producer: {e}")
    
    def send_message_with_retry(self, topic: str, message: Dict[Any, Any], 
                               key: Optional[str] = None) -> bool:
        """Send message with retry logic and DLQ fallback"""
        
        # Add metadata for tracking
        enriched_message = {
            **message,
            '_metadata': {
                'producer_timestamp': datetime.now().isoformat(),
                'attempt_count': 0,
                'original_topic': topic
            }
        }
        
        for attempt in range(self.max_retries + 1):
            try:
                enriched_message['_metadata']['attempt_count'] = attempt + 1
                
                future = self.producer.send(topic, value=enriched_message, key=key)
                record_metadata = future.get(timeout=30)
                
                logger.info(f"Message sent successfully: topic={record_metadata.topic}, "
                           f"partition={record_metadata.partition}, offset={record_metadata.offset}")
                return True
                
            except KafkaTimeoutError as e:
                logger.warning(f"Timeout on attempt {attempt + 1}/{self.max_retries + 1}: {e}")
                if attempt < self.max_retries:
                    time.sleep(2 ** attempt)  # Exponential backoff
                    continue
                else:
                    return self._send_to_dlq(topic, enriched_message, key, str(e))
            
            except KafkaError as e:
                logger.error(f"Kafka error on attempt {attempt + 1}: {e}")
                if attempt < self.max_retries:
                    time.sleep(2 ** attempt)
                    continue
                else:
                    return self._send_to_dlq(topic, enriched_message, key, str(e))
            
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                return self._send_to_dlq(topic, enriched_message, key, str(e))
        
        return False
    
    def _send_to_dlq(self, original_topic: str, message: Dict[Any, Any], 
                     key: Optional[str], error_msg: str) -> bool:
        """Send failed message to Dead Letter Queue"""
        if not self.dlq_producer:
            logger.error("DLQ producer not available")
            return False
        
        dlq_message = {
            'original_topic': original_topic,
            'original_key': key,
            'original_message': message,
            'error_message': error_msg,
            'failed_at': datetime.now().isoformat(),
            'dlq_reason': 'max_retries_exceeded'
        }
        
        try:
            dlq_topic = f"{original_topic}-dlq"
            future = self.dlq_producer.send(dlq_topic, value=dlq_message, key=key)
            future.get(timeout=10)
            logger.warning(f"Message sent to DLQ: {dlq_topic}")
            return True
        except Exception as e:
            logger.error(f"Failed to send to DLQ: {e}")
            return False
    
    def close(self):
        if self.producer:
            self.producer.close()
        if self.dlq_producer:
            self.dlq_producer.close()

class RobustConsumer:
    def __init__(self, topics, group_id, bootstrap_servers=['localhost:9092']):
        self.consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='earliest',
            enable_auto_commit=False,  # Manual commit for better control
            max_poll_records=10  # Process smaller batches for better error handling
        )
        self.error_count = 0
        self.max_errors = 5
        self.processed_count = 0
    
    def process_message(self, message) -> bool:
        """Process individual message with error handling"""
        try:
            # Simulate message processing
            data = message.value
            key = message.key
            
            # Example processing logic
            if not isinstance(data, dict):
                raise ValueError("Message must be a dictionary")
            
            # Check for required fields
            if '_metadata' not in data:
                logger.warning("Message missing metadata")
            
            # Simulate processing time
            time.sleep(0.1)
            
            logger.info(f"Processed message: key={key}, topic={message.topic}")
            return True
            
        except ValueError as e:
            logger.error(f"Validation error: {e}")
            return False
        except Exception as e:
            logger.error(f"Processing error: {e}")
            return False
    
    def consume_with_error_handling(self):
        """Consume messages with comprehensive error handling"""
        logger.info("Starting robust consumer...")
        
        try:
            while True:
                try:
                    # Poll for messages
                    message_batch = self.consumer.poll(timeout_ms=1000)
                    
                    if not message_batch:
                        continue
                    
                    # Process each partition's messages
                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            success = self.process_message(message)
                            
                            if success:
                                self.processed_count += 1
                                self.error_count = 0  # Reset error count on success
                            else:
                                self.error_count += 1
                                logger.warning(f"Failed to process message, error count: {self.error_count}")
                                
                                # Handle too many errors
                                if self.error_count >= self.max_errors:
                                    logger.error("Too many consecutive errors, pausing...")
                                    time.sleep(10)
                                    self.error_count = 0
                    
                    # Commit offsets after successful processing
                    try:
                        self.consumer.commit()
                    except CommitFailedError as e:
                        logger.error(f"Commit failed: {e}")
                
                except Exception as e:
                    logger.error(f"Consumer poll error: {e}")
                    time.sleep(5)  # Brief pause before retrying
                
        except KeyboardInterrupt:
            logger.info("Consumer shutdown requested")
        except Exception as e:
            logger.error(f"Fatal consumer error: {e}")
        finally:
            self.close()
    
    def close(self):
        logger.info(f"Closing consumer. Processed {self.processed_count} messages.")
        self.consumer.close()

# Example usage and testing
def simulate_error_scenarios():
    """Simulate various error scenarios for testing"""
    producer = RobustProducer()
    
    try:
        # Test normal message
        normal_message = {
            'type': 'normal',
            'content': 'This should work fine',
            'timestamp': datetime.now().isoformat()
        }
        producer.send_message_with_retry('error-handling', normal_message, 'normal-key')
        
        # Test with invalid topic (this might fail)
        invalid_message = {
            'type': 'invalid_topic_test',
            'content': 'Testing invalid topic handling'
        }
        producer.send_message_with_retry('invalid-topic-name-that-does-not-exist', 
                                       invalid_message, 'invalid-key')
        
        # Test large message
        large_message = {
            'type': 'large',
            'content': 'x' * 1000,  # Large content
            'bulk_data': ['item'] * 1000
        }
        producer.send_message_with_retry('error-handling', large_message, 'large-key')
        
    finally:
        producer.close()

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == 'consumer':
        # Run consumer
        consumer = RobustConsumer(['error-handling', 'error-handling-dlq'], 'error-handling-group')