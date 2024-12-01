import json
import time
import subprocess
import logging
from kafka import KafkaProducer
import pandas as pd
import csv

# Logging Configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Kafka Configuration
KAFKA_BROKER = 'localhost:9092'  # Update with the appropriate host
KAFKA_INDICATORS_TOPIC = 'indicators_topic'

CSV_FILE_PATH = r"C:/Users/I745988/Ransomware-attack/data/sub_final.csv"  # Path to the CSV file
ROW_LIMIT = 10000000000  # Limit the number of rows fetched (you can adjust this based on need)

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

# Fetch data from CSV file
def fetch_csv_data(csv_file, row_limit):
    try:
        logger.info("Reading data from CSV file '%s'...", csv_file)

        # Read the CSV file into a pandas DataFrame
        df = pd.read_csv(csv_file)

        # If row_limit is set, limit the number of rows
        if row_limit:
            df = df.head(row_limit)

        logger.info("Successfully read %d rows from the CSV file.", len(df))

        # Convert the DataFrame to a list of dictionaries for easy use
        data = df.to_dict(orient='records')

        return df.columns.tolist(), data

    except Exception as e:
        logger.error("An error occurred while reading the CSV file: %s", e)
        return None, None

# Kafka Producer Configuration
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Produce data to Kafka
def produce_data_to_kafka():
    while True:
        logger.info("Starting data ingestion process...")

        # Fetch data from CSV file
        columns, data = fetch_csv_data(CSV_FILE_PATH, ROW_LIMIT)

        if not data:
            logger.warning("No data to send to Kafka. Skipping this cycle.")
            time.sleep(5)
            continue

        # Send the data to Kafka - 'indicators_topic'
        logger.info("Sending data to Kafka topic '%s'...", KAFKA_INDICATORS_TOPIC)
        for row in data:
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
