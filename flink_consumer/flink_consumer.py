import json
import logging
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point
from collections import Counter
import time

# Logging Configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Kafka Configuration
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'indicators_topic'

INFLUXDB_DB="ransomeware"
INFLUXDB_HOST="http://localhost:8086"
AUTH_TOKEN="cnPhRh4jqqJgyAZciIP-Ulzkx_X76VqbK2MApufABa_WF_1Bdk0j3wOmFEorb2gOFGFF4ulMqsbOGMIWuIppQg==" 
DEFAULT_BUCKET="ransomeware"
INFLUXDB_BUCKET ="ransomeware"
MONITORING_BUCKET="ransomeware"
DEFAULT_ORGANIZATION="ransomeware"
INFLUXDB_ORG = "ransomeware"
ADMIN_USERNAME="admin"
ADMIN_PASSWORD="admin123"

# Initialize InfluxDB Client
influx_client = InfluxDBClient(url=INFLUXDB_HOST, token=AUTH_TOKEN, org=DEFAULT_ORGANIZATION)
write_api = influx_client.write_api()  # Use default WriteOptions

# Kafka Consumer
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

# Counters for aggregation
top_targets = Counter()
top_sources = Counter()

def write_to_influx(measurement, data):
    """
    Write aggregated data to InfluxDB.
    """
    try:
        points = []
        for country, count in data:
            point = Point(measurement).tag("country", country).field("count", count)
            points.append(point)
        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=points)
        logger.info(f"Data written to InfluxDB measurement '{measurement}'.")
    except Exception as e:
        logger.error("Error writing to InfluxDB: %s", str(e))

def process_kafka_stream():
    """
    Process incoming Kafka stream and update InfluxDB with aggregated data.
    """
    try:
        for message in consumer:
            # Extract message data
            data = message.value
            target_country = data.get('location_country', 'Unknown')
            source_country = data.get('pulse_author_name', 'Unknown')

            # Update counters
            top_targets[target_country] += 1
            top_sources[source_country] += 1

            # Get top 10 countries
            top_10_targets = top_targets.most_common(10)
            top_10_sources = top_sources.most_common(10)

            # Log the top 10 results
            logger.info(f"Top 10 Targets: {top_10_targets}")
            logger.info(f"Top 10 Sources: {top_10_sources}")

            # Write results to InfluxDB
            write_to_influx("top_targets", top_10_targets)
            write_to_influx("top_sources", top_10_sources)

            # Simulate delay for processing
            time.sleep(1)

    except KeyboardInterrupt:
        logger.info("Kafka Consumer interrupted.")
    finally:
        consumer.close()
        logger.info("Kafka Consumer closed.")

if __name__ == "__main__":
    logger.info("Starting Kafka Consumer for topic '%s'...", TOPIC_NAME)
    process_kafka_stream()
