import json
from kafka import KafkaProducer

# Kafka configuration
KAFKA_BROKER = 'localhost:29092'  # Replace with your Kafka broker address
TOPIC = 'input-topic'            # Replace with your Kafka topic name

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize JSON data
)

# Read and preprocess the JSON file

with open('dummy_data.json', 'r') as file:
    raw_data = json.load(file)


# Extract columns and records
columns = raw_data["columns"]
records = raw_data["records"]

# Convert records into individual JSON objects
for record in records:
    message = dict(zip(columns, record))  # Map columns to record values
    producer.send(TOPIC, value=message)   # Send each message to Kafka
    print(f"Produced: {message}")

# Close the producer
producer.flush()
producer.close()
