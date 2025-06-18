import asyncio
from aiokafka import AIOKafkaProducer
import json
from datetime import datetime

class AsyncProducer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
    
    async def start(self):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8') if k else None
        )
        await self.producer.start()
    
    async def send_message(self, topic, message, key=None):
        try:
            result = await self.producer.send(topic, value=message, key=key)
            print(f"Message sent to {result.topic} partition {result.partition} offset {result.offset}")
            return True
        except Exception as e:
            print(f"Error sending message: {e}")
            return False
    
    async def send_multiple_messages(self, topic, messages):
        tasks = []
        for i, message in enumerate(messages):
            task = self.send_message(topic, message, f"async-key-{i}")
            tasks.append(task)
        
        results = await asyncio.gather(*tasks)
        return results
    
    async def stop(self):
        if self.producer:
            await self.producer.stop()

async def main():
    producer = AsyncProducer()
    
    try:
        await producer.start()
        
        # Create sample messages
        messages = [
            {
                'id': i,
                'message': f"Async message #{i}",
                'timestamp': datetime.now().isoformat()
            }
            for i in range(5)
        ]
        
        # Send messages concurrently
        print("Sending messages asynchronously...")
        await producer.send_multiple_messages('test-topic', messages)
        
    finally:
        await producer.stop()

if __name__ == "__main__":
    # Install aiokafka first: pip install aiokafka
    asyncio.run(main())