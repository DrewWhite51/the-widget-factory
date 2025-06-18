# Kafka Python Quickstart Project

A complete Apache Kafka implementation with Python producers and consumers, designed for learning and development purposes.

## 🚀 Quick Start

```bash
# 1. Clone/create project directory
mkdir kafka-python-project && cd kafka-python-project

# 2. Set up virtual environment
python3 -m venv venv
source venv/bin/activate

# 3. Install dependencies
pip install kafka-python aiokafka

# 4. Start Kafka services
docker-compose up -d

# 5. Create topic
docker exec -it kafka kafka-topics --create --topic test-topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# 6. Run consumer (terminal 1)
python consumer.py

# 7. Run producer (terminal 2)
python producer.py
```

## 📋 Prerequisites

- **Python 3.7+**
- **Docker & Docker Compose**
- **Git** (optional)

### System Requirements
- **RAM**: 4GB minimum (8GB recommended)
- **Disk**: 2GB free space
- **OS**: Linux, macOS, or Windows with WSL2

## 🏗️ Project Structure

```
kafka-python-project/
├── docker-compose.yml          # Kafka infrastructure
├── producer.py                 # Synchronous message producer
├── consumer.py                 # Message consumer with graceful shutdown
├── async_producer.py           # Asynchronous high-throughput producer
├── requirements.txt            # Python dependencies
├── README.md                   # This file
├── scripts/
│   ├── create-topics.sh        # Topic creation script
│   ├── reset-kafka.sh          # Clean reset script
│   └── monitor.sh              # Monitoring script
└── examples/
    ├── batch_producer.py       # Batch processing example
    ├── avro_example.py         # Schema registry example
    └── error_handling.py       # Error handling patterns
```

## 🔧 Installation & Setup

### 1. Environment Setup

**For Ubuntu/Debian systems with externally-managed Python:**
```bash
# Create isolated environment
python3 -m venv venv
source venv/bin/activate

# Verify activation
which python  # Should show path with 'venv'
```

**For other systems:**
```bash
pip install kafka-python aiokafka
```

### 2. Start Kafka Infrastructure

```bash
# Start all services
docker-compose up -d

# Verify services are running
docker-compose ps

# Check logs if needed
docker-compose logs kafka
```

## 📦 Dependencies

Create `requirements.txt`:
```txt
kafka-python==2.0.2
aiokafka==0.8.11
```

Install with:
```bash
pip install -r requirements.txt
```

## 🎯 Core Components

### Producer (`producer.py`)
- **JSON serialization** for message payloads
- **Key-based partitioning** for message ordering
- **Batch processing** capabilities
- **Error handling** with retry logic

### Consumer (`consumer.py`)
- **Consumer groups** for scalable processing
- **Graceful shutdown** handling (Ctrl+C)
- **Automatic offset management**
- **Detailed message logging**

### Async Producer (`async_producer.py`)
- **High-throughput** message sending
- **Concurrent operations** using asyncio
- **Non-blocking** I/O operations

## 🔨 Usage Examples

### Basic Producer Usage
```python
from producer import SimpleProducer

producer = SimpleProducer()
message = {'user_id': 123, 'action': 'login'}
producer.send_message('user-events', message, key='user-123')
producer.close()
```

### Basic Consumer Usage
```python
from consumer import SimpleConsumer

consumer = SimpleConsumer(['user-events'], group_id='analytics-group')
consumer.consume_messages()  # Runs until Ctrl+C
```

### Async Producer for High Throughput
```python
import asyncio
from async_producer import AsyncProducer

async def send_bulk_messages():
    producer = AsyncProducer()
    await producer.start()
    
    messages = [{'id': i, 'data': f'message-{i}'} for i in range(1000)]
    await producer.send_multiple_messages('bulk-topic', messages)
    
    await producer.stop()

asyncio.run(send_bulk_messages())
```

## 🛠️ Kafka Management Commands

### Topic Operations
```bash
# List all topics
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092

# Create topic with specific configuration
docker exec -it kafka kafka-topics --create \
    --topic my-topic \
    --bootstrap-server localhost:9092 \
    --partitions 6 \
    --replication-factor 1 \
    --config retention.ms=86400000

# Delete topic
docker exec -it kafka kafka-topics --delete --topic my-topic --bootstrap-server localhost:9092

# Describe topic details
docker exec -it kafka kafka-topics --describe --topic test-topic --bootstrap-server localhost:9092
```

### Consumer Groups
```bash
# List consumer groups
docker exec -it kafka kafka-consumer-groups --list --bootstrap-server localhost:9092

# Describe consumer group
docker exec -it kafka kafka-consumer-groups --describe --group test-group --bootstrap-server localhost:9092

# Reset consumer group offset
docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
    --group test-group --topic test-topic --reset-offsets --to-earliest --execute
```

### Monitoring & Debugging
```bash
# View messages from beginning
docker exec -it kafka kafka-console-consumer \
    --topic test-topic \
    --from-beginning \
    --bootstrap-server localhost:9092

# Monitor real-time messages
docker exec -it kafka kafka-console-consumer \
    --topic test-topic \
    --bootstrap-server localhost:9092

# Send test message via console
docker exec -it kafka kafka-console-producer \
    --topic test-topic \
    --bootstrap-server localhost:9092
```

## ⚙️ Configuration

### Producer Configuration
```python
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    acks='all',                    # Wait for all replicas
    retries=3,                     # Retry failed sends
    batch_size=16384,              # Batch messages for efficiency
    linger_ms=10,                  # Wait time before sending batch
    buffer_memory=33554432,        # Total memory for buffering
    max_request_size=1048576,      # Max size of request
)
```

### Consumer Configuration
```python
consumer = KafkaConsumer(
    'my-topic',
    bootstrap_servers=['localhost:9092'],
    group_id='my-group',
    auto_offset_reset='earliest',   # Start from beginning
    enable_auto_commit=True,        # Auto-commit offsets
    auto_commit_interval_ms=1000,   # Commit frequency
    max_poll_records=500,           # Records per poll
    session_timeout_ms=30000,       # Session timeout
)
```

## 🚨 Troubleshooting

### Common Issues

**1. Connection Refused**
```bash
# Check if Kafka is running
docker-compose ps

# Check Kafka logs
docker-compose logs kafka

# Restart services
docker-compose restart
```

**2. Topic Not Found**
```bash
# Create the topic first
docker exec -it kafka kafka-topics --create --topic your-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

**3. Consumer Not Receiving Messages**
```bash
# Check consumer group status
docker exec -it kafka kafka-consumer-groups --describe --group your-group --bootstrap-server localhost:9092

# Reset offset to beginning
docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:9092 --group your-group --topic your-topic --reset-offsets --to-earliest --execute
```

**4. Port Already in Use**
```bash
# Check what's using port 9092
sudo lsof -i :9092

# Stop conflicting services
docker-compose down
```

**5. Virtual Environment Issues**
```bash
# Deactivate and recreate
deactivate
rm -rf venv
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## 📊 Performance Tips

### Producer Optimization
- **Batch messages** for better throughput
- **Use async producers** for high-volume scenarios
- **Compress messages** with `compression_type='gzip'`
- **Tune batch_size** and `linger_ms` for your use case

### Consumer Optimization
- **Use multiple consumers** in the same group for parallel processing
- **Adjust max_poll_records** based on message size
- **Implement proper error handling** for message processing
- **Consider manual offset commits** for exactly-once processing

## 🔒 Production Considerations

### Security
- Enable **SSL/TLS** encryption
- Configure **SASL authentication**
- Set up **ACLs** for topic access control
- Use **secrets management** for credentials

### Monitoring
- Set up **JMX metrics** collection
- Monitor **consumer lag**
- Track **broker performance**
- Implement **alerting** for critical metrics

### Scaling
- **Increase partitions** for higher parallelism
- **Add more brokers** for fault tolerance
- **Tune JVM settings** for broker performance
- **Use dedicated Zookeeper** cluster

## 📚 Learning Resources

- **[Apache Kafka Documentation](https://kafka.apache.org/documentation/)**
- **[Confluent Developer Guides](https://developer.confluent.io/)**
- **[kafka-python Documentation](https://kafka-python.readthedocs.io/)**
- **[Kafka Patterns and Anti-patterns](https://www.confluent.io/blog/kafka-anti-patterns/)**

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 📞 Support

- **Issues**: Create a GitHub issue
- **Questions**: Use the discussions tab
- **Email**: your-email@domain.com

---

**Happy Kafka-ing! 🎉**