from kafka import KafkaConsumer
import json
import logging
import time

# Logging Configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Kafka Configuration
KAFKA_BROKER = 'localhost:9092'  # Update if necessary
TOPIC_NAME = 'indicators_topic'

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',  # Start reading from the beginning of the topic
    enable_auto_commit=True
)

# Dictionary to count occurrences of countries
country_count = {}

def process_data(message):
    """
    Processes a Kafka message to update and log the top countries.

    Args:
        message (dict): The Kafka message containing the data.
    """
    try:
        # Extract necessary fields
        indicator_id = message.get('indicator_id', 'N/A')
        indicator_type = message.get('indicator_type', 'Unknown')
        location_country = message.get('location_country', 'Unknown')
        pulse_malware_family = message.get('pulse_malware_family', 'Unknown')

        # Update country count
        country_count[location_country] = country_count.get(location_country, 0) + 1

        # Sort and display the top 10 countries by count
        top_countries = sorted(country_count.items(), key=lambda x: x[1], reverse=True)[:10]
        logger.info("Top Countries: %s", top_countries)

        # Log additional details (optional)
        logger.info(
            "Processed Indicator: ID=%s, Type=%s, Country=%s, Malware Family=%s",
            indicator_id,
            indicator_type,
            location_country,
            pulse_malware_family
        )
    except Exception as e:
        logger.error("Error processing message: %s", str(e))

if __name__ == "__main__":
    logger.info("Starting Kafka Consumer for topic '%s'...", TOPIC_NAME)

    try:
        for message in consumer:
            # Extract and process the message value
            data = message.value
            logger.info("Received message: %s", data)
            process_data(data)
            time.sleep(1)  # Simulate processing delay
    except KeyboardInterrupt:
        logger.info("Consumer stopped.")
    finally:
        consumer.close()
        logger.info("Kafka Consumer closed.")
