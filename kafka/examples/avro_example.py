import json
import io
from kafka import KafkaProducer, KafkaConsumer
from datetime import datetime
import logging

# Note: For full Avro support, install confluent-kafka-python and avro-python3
# pip install confluent-kafka avro-python3

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Simulated schema registry (in production, use Confluent Schema Registry)
USER_SCHEMA = {
    "type": "record",
    "name": "User",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "name", "type": "string"},
        {"name": "email", "type": "string"},
        {"name": "created_at", "type": "string"},
        {"name": "metadata", "type": ["null", {
            "type": "record",
            "name": "UserMetadata",
            "fields": [
                {"name": "source", "type": "string"},
                {"name": "version", "type": "string"}
            ]
        }], "default": None}
    ]
}

ORDER_SCHEMA = {
    "type": "record",
    "name": "Order",
    "fields": [
        {"name": "order_id", "type": "string"},
        {"name": "user_id", "type": "int"},
        {"name": "amount", "type": "double"},
        {"name": "currency", "type": "string"},
        {"name": "items", "type": {
            "type": "array",
            "items": {
                "type": "record",
                "name": "OrderItem",
                "fields": [
                    {"name": "product_id", "type": "string"},
                    {"name": "quantity", "type": "int"},
                    {"name": "price", "type": "double"}
                ]
            }
        }},
        {"name": "timestamp", "type": "string"}
    ]
}

class SchemaRegistry:
    """Simple in-memory schema registry simulation"""
    
    def __init__(self):
        self.schemas = {
            'user-schema': USER_SCHEMA,
            'order-schema': ORDER_SCHEMA
        }
    
    def get_schema(self, schema_name: str):
        return self.schemas.get(schema_name)
    
    def validate_message(self, message: dict, schema_name: str) -> bool:
        """Basic validation - in production use proper Avro validation"""
        schema = self.get_schema(schema_name)
        if not schema:
            return False
        
        # Simple field validation
        required_fields = [field['name'] for field in schema['fields'] 
                          if 'default' not in field]
        
        for field in required_fields:
            if field not in message:
                logger.error(f"Missing required field: {field}")
                return False
        
        return True

class SchemaAwareProducer:
    def __init__(self, bootstrap_servers=['localhost:9092']):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=self._serialize_with_schema,
            key_serializer=lambda k: str(k).encode('utf-8') if k else None
        )
        self.schema_registry = SchemaRegistry()
    
    def _serialize_with_schema(self, message_with_schema):
        """Serialize message with schema validation"""
        message, schema_name = message_with_schema
        
        # Validate against schema
        if not self.schema_registry.validate_message(message, schema_name):
            raise ValueError(f"Message validation failed for schema: {schema_name}")
        
        # In production, use proper Avro serialization
        # For now, we'll use JSON with schema metadata
        envelope = {
            'schema': schema_name,
            'data': message,
            'version': '1.0'
        }
        
        return json.dumps(envelope).encode('utf-8')
    
    def send_user_event(self, user_data: dict, key: str = None):
        """Send user event with schema validation"""
        try:
            future = self.producer.send(
                'user-events',
                value=(user_data, 'user-schema'),
                key=key
            )
            result = future.get(timeout=10)
            logger.info(f"User event sent: partition {result.partition}, offset {result.offset}")
            return True
        except Exception as e:
            logger.error(f"Failed to send user event: {e}")
            return False
    
    def send_order_event(self, order_data: dict, key: str = None):
        """Send order event with schema validation"""
        try:
            future = self.producer.send(
                'order-events',
                value=(order_data, 'order-schema'),
                key=key
            )
            result = future.get(timeout=10)
            logger.info(f"Order event sent: partition {result.partition}, offset {result.offset}")
            return True
        except Exception as e:
            logger.error(f"Failed to send order event: {e}")
            return False
    
    def close(self):
        self.producer.close()

class SchemaAwareConsumer:
    def __init__(self, topics, group_id, bootstrap_servers=['localhost:9092']):
        self.consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            value_deserializer=self._deserialize_with_schema,
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='earliest'
        )
        self.schema_registry = SchemaRegistry()
    
    def _deserialize_with_schema(self, message_bytes):
        """Deserialize message and validate schema"""
        try:
            envelope = json.loads(message_bytes.decode('utf-8'))
            schema_name = envelope.get('schema')
            data = envelope.get('data')
            
            # Validate schema exists
            if not self.schema_registry.get_schema(schema_name):
                raise ValueError(f"Unknown schema: {schema_name}")
            
            return {
                'schema': schema_name,
                'data': data,
                'version': envelope.get('version', '1.0')
            }
        except Exception as e:
            logger.error(f"Deserialization error: {e}")
            return None
    
    def consume_messages(self):
        """Consume messages with schema awareness"""
        logger.info("Starting schema-aware consumer...")
        
        try:
            for message in self.consumer:
                if message.value is None:
                    continue
                
                schema_name = message.value['schema']
                data = message.value['data']
                
                logger.info(f"Received {schema_name} message:")
                logger.info(f"  Key: {message.key}")
                logger.info(f"  Data: {json.dumps(data, indent=2)}")
                logger.info(f"  Partition: {message.partition}, Offset: {message.offset}")
                logger.info("-" * 50)
                
        except KeyboardInterrupt:
            logger.info("Consumer stopped")
        finally:
            self.consumer.close()

def create_sample_data():
    """Create sample data that conforms to schemas"""
    
    # Sample user data
    user_data = {
        "id": 12345,
        "name": "John Doe",
        "email": "john.doe@example.com",
        "created_at": datetime.now().isoformat(),
        "metadata": {
            "source": "web_signup",
            "version": "2.1"
        }
    }
    
    # Sample order data
    order_data = {
        "order_id": "order_789",
        "user_id": 12345,
        "amount": 99.99,
        "currency": "USD",
        "items": [
            {
                "product_id": "prod_001",
                "quantity": 2,
                "price": 29.99
            },
            {
                "product_id": "prod_002",
                "quantity": 1,
                "price": 39.99
            }
        ],
        "timestamp": datetime.now().isoformat()
    }
    
    return user_data, order_data

if __name__ == "__main__":
    # Producer example
    producer = SchemaAwareProducer()
    
    try:
        user_data, order_data = create_sample_data()
        
        # Send user event
        producer.send_user_event(user_data, key=str(user_data['id']))
        
        # Send order event
        producer.send_order_event(order_data, key=order_data['order_id'])
        
        logger.info("Sample events sent successfully!")
        
    except Exception as e:
        logger.error(f"Producer error: {e}")
    finally:
        producer.close()