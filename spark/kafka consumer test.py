from kafka import KafkaConsumer

# Create a consumer
# Kafka Configuration
KAFKA_BROKER = 'localhost:9092'
KAFKA_INDICATORS_TOPIC = 'indicators_topic_API'
consumer = KafkaConsumer(
    KAFKA_INDICATORS_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='earliest',  # Read messages from the beginning
    enable_auto_commit=True,
    group_id='my-group'
)

print("Reading messages...")
for message in consumer:
    print(f"Key: {message.key}, Value: {message.value}")
