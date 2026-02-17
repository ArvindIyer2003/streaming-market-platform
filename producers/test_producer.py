"""
Quick test to verify Kafka is working
"""
from kafka import KafkaProducer
import json
from datetime import datetime

# Create producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Send test message
test_message = {
    "symbol": "TCS.NS",
    "price": 3500.50,
    "timestamp": datetime.now().isoformat(),
    "test": True
}

producer.send('stock-prices', value=test_message)
producer.flush()

print("✅ Test message sent to Kafka!")
print(f"Message: {test_message}")
print("Check Kafka UI at http://localhost:8080 to see the message!")