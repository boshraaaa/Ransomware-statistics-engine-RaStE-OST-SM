import sqlite3
import json
import time
import subprocess
import logging
from kafka import KafkaProducer

# Logging Configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Kafka Configuration
KAFKA_BROKER = 'localhost:9092'  # Update with the appropriate host
KAFKA_INDICATORS_TOPIC = 'indicators_topic'

DB_FILE = r"C:/Users/I745988/Downloads/2023-10-25_cti_data_majd/2023-10-25_cti_data_majd.db"  # Update with the correct path to your SQLite DB file
ROW_LIMIT = 10  # Limit the number of rows fetched

# Kafka Topic Creation Check
def check_and_create_kafka_topic(topic_name):
    try:
        # List existing topics
        logger.info("Checking if Kafka topic '%s' exists...", topic_name)
        command = f"docker exec kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server kafka:9092"
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        existing_topics = result.stdout.splitlines()

        # If topic doesn't exist, create it
        if topic_name not in existing_topics:
            logger.info("Topic '%s' does not exist. Creating it...", topic_name)
            create_command = f"docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic {topic_name} --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1"
            subprocess.run(create_command, shell=True)
            logger.info("Topic '%s' created successfully.", topic_name)
        else:
            logger.info("Topic '%s' already exists.", topic_name)
    except Exception as e:
        logger.error("Error checking or creating Kafka topic: %s", str(e))

# Fetch data from SQLite and join 'indicators', 'pulses', and 'ip_location'
def fetch_joined_data(db_file, row_limit):
    try:
        logger.info("Connecting to SQLite database '%s'...", db_file)
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        # SQL query to join three tables: 'indicators', 'pulses', 'ip_location'
        logger.info("Fetching data from SQLite with join query...")
        join_query = """
        SELECT 
            ind.id AS indicator_id,
            ind.pulse_id,
            ind.indicator,
            ind.type AS indicator_type,
            ind.created AS indicator_created,
            ind.title AS indicator_title,
            ind.description AS indicator_description,
            ind.expiration AS indicator_expiration,
            ind.is_active AS indicator_is_active,
            pul.id AS pulse_id,
            pul.name AS pulse_name,
            pul.description AS pulse_description,
            pul.author_name AS pulse_author_name,
            pul.modified AS pulse_modified,
            pul.created AS pulse_created,
            pul.public AS pulse_public,
            pul.adversary AS pulse_adversary,
            pul.TLP AS pulse_TLP,
            pul.revision AS pulse_revision,
            pul.in_group AS pulse_in_group,
            pul.is_subscribing AS pulse_is_subscribing,
            pul.malware_family AS pulse_malware_family,
            loc.cityName AS location_city,
            loc.countryName AS location_country,
            loc.latitude AS location_latitude,
            loc.longitude AS location_longitude
        FROM 
            indicators AS ind
        INNER JOIN 
            pulses AS pul
        ON 
            ind.pulse_id = pul.id
        INNER JOIN
            ip_location AS loc
        ON
            ind.indicator = loc.ip
        LIMIT ?;
        """
        cursor.execute(join_query, (row_limit,))
        joined_rows = cursor.fetchall()

        # Get column names for joined data
        joined_columns = [description[0] for description in cursor.description]

        # Convert rows to dictionary for easier usage
        joined_data = [dict(zip(joined_columns, row)) for row in joined_rows]

        cursor.close()
        conn.close()

        logger.info("Successfully fetched %d rows from the database.", len(joined_data))

        return joined_columns, joined_data
    except sqlite3.Error as e:
        logger.error("Error while fetching data from SQLite: %s", str(e))
        return [], []

# Kafka Producer Configuration
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Produce data to Kafka
def produce_data_to_kafka():
    while True:
        logger.info("Starting data ingestion process...")
        
        # Fetch joined data
        joined_columns, joined_data = fetch_joined_data(DB_FILE, ROW_LIMIT)
        
        if not joined_data:
            logger.warning("No data to send to Kafka. Skipping this cycle.")
            time.sleep(5)
            continue

        # Send joined data to Kafka - 'indicators_topic'
        logger.info("Sending data to Kafka topic '%s'...", KAFKA_INDICATORS_TOPIC)
        for row in joined_data:
            try:
                producer.send(KAFKA_INDICATORS_TOPIC, value=row)
                logger.info("Sent data to Kafka: %s", row)
            except Exception as e:
                logger.error("Error sending data to Kafka: %s", str(e))

        # Sleep for a while before fetching new data
        time.sleep(5)

if __name__ == "__main__":
    # Verify Kafka topics and create if not exist
    check_and_create_kafka_topic(KAFKA_INDICATORS_TOPIC)
    produce_data_to_kafka()
