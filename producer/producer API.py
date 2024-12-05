import json
import time
import subprocess
import logging
from kafka import KafkaProducer
from OTXv2 import OTXv2, IndicatorTypes

# Logging Configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Kafka Configuration
KAFKA_BROKER = 'localhost:9092'
KAFKA_INDICATORS_TOPIC = 'indicators_topic_API'
PULSE_LIMIT = 10

# API Configuration
CONFIG_FILE = "C:/Users/I745988/Ransomware-attack/producer/config.json"
OTX_SERVER = 'https://otx.alienvault.com/'

# Load API Key
def load_api_key(config_path):
    try:
        with open(config_path, "r") as file:
            config = json.load(file)
            return config.get("API_KEY")
    except Exception as e:
        logger.error("Failed to load API key from config file: %s", e)
        return None

# Initialize OTX API Client
API_KEY = load_api_key(CONFIG_FILE)
if not API_KEY:
    raise RuntimeError("API key is missing. Please provide a valid key in the config file.")

otx = OTXv2(API_KEY, server=OTX_SERVER)

# Kafka Topic Creation Check
def check_and_create_kafka_topic(topic_name):
    try:
        logger.info("Checking if Kafka topic '%s' exists...", topic_name)
        command = f"docker exec kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server kafka:9092"
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        existing_topics = result.stdout.splitlines()

        if topic_name not in existing_topics:
            logger.info("Topic '%s' does not exist. Creating it...", topic_name)
            create_command = f"docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic {topic_name} --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1"
            subprocess.run(create_command, shell=True)
            logger.info("Topic '%s' created successfully.", topic_name)
        else:
            logger.info("Topic '%s' already exists.", topic_name)
    except Exception as e:
        logger.error("Error checking or creating Kafka topic: %s", str(e))

# Fetch Latest Pulses
def fetch_pulses(limit=PULSE_LIMIT):
    try:
        logger.info("Fetching the latest %d pulses...", limit)
        pulses = otx.getall(max_items=limit, max_page=10)
        logger.info("Successfully fetched %d pulses.", len(pulses))
        return pulses
    except Exception as e:
        logger.error("Failed to fetch pulses: %s", e)
        return []

# Fetch Indicators for a Pulse
def fetch_indicators_for_pulse(pulse_id):
    try:
        logger.info("Fetching indicators for pulse ID: %s", pulse_id)
        pulse_details = otx.get_pulse_details(pulse_id)
        if not pulse_details or "indicators" not in pulse_details:
            logger.warning("No indicators found for pulse ID: %s", pulse_id)
            return []
        return pulse_details["indicators"]
    except Exception as e:
        logger.error("Failed to fetch indicators for pulse ID %s: %s", pulse_id, e)
        return []

# Safely Get Nested Fields
def safe_get(data, keys, default="unknown"):
    try:
        for key in keys:
            if isinstance(data, list):
                key = int(key)
            data = data[key]
        return data
    except (KeyError, IndexError, TypeError, ValueError):
        return default

def get_indicator_type_by_name(name):
    for indicator in IndicatorTypes.all_types:
        if indicator.name == name:
            return indicator
    raise ValueError(f"Invalid indicator type name: {name}")

# Fetch Facts for Indicators and Send to Kafka
def fetch_facts_for_indicators(indicators, producer):
    for indicator_data in indicators:
        try:
            indicator_value = indicator_data.get("indicator", "unknown")
            indicator_type = indicator_data.get("type", "unknown")

            if indicator_value != "unknown" and indicator_type != "unknown":
                logger.info("Fetching facts for indicator: %s (type: %s)", indicator_value, indicator_type)
                fact = otx.get_indicator_details_full(get_indicator_type_by_name(indicator_type), indicator_value)

                # Safely retrieve all relevant fields
                formatted_fact = {
                    "id_indicator": safe_get(fact, ["general", "base_indicator", "id"]),
                    "indicator": safe_get(fact, ["general", "base_indicator", "indicator"]),
                    "type": safe_get(fact, ["general", "base_indicator", "type"]),
                    "created_indicator": safe_get(fact, ["general", "base_indicator", "created"], "2023-05-11T07:32:46"),
                    "content": safe_get(fact, ["general", "base_indicator", "content"]),
                    "title": safe_get(fact, ["general", "base_indicator", "title"]),
                    "description_indicator": safe_get(fact, ["general", "base_indicator", "description"]),
                    "expiration": safe_get(fact, ["general", "base_indicator", "expiration"]),
                    "is_active": safe_get(fact, ["general", "pulse_info", "pulses", "0", "related_indicator_is_active"], 1),
                    "id_pulse": safe_get(fact, ["general", "pulse_info", "pulses", "0", "id"]),
                    "name": safe_get(fact, ["general", "pulse_info", "pulses", "0", "name"]),
                    "description_pulse": safe_get(fact, ["general", "pulse_info", "pulses", "0", "description"]),
                    "author_name": safe_get(fact, ["general", "pulse_info", "pulses", "0", "author_name"]),
                    "modified": safe_get(fact, ["general", "pulse_info", "pulses", "0", "modified"], "2023-05-11T07:32:19.907000"),
                    "created_pulse": safe_get(fact, ["general", "pulse_info", "pulses", "0", "created"], "2023-05-11T07:32:19.907000"),
                    "public": safe_get(fact, ["general", "pulse_info", "pulses", "0", "public"], 1),
                    "adversary": safe_get(fact, ["general", "pulse_info", "related", "alienvault", "adversary"]),
                    "TLP": safe_get(fact, ["general", "pulse_info", "pulses", "0", "TLP"], "white"),
                    "revision": safe_get(fact, ["general", "pulse_info", "pulses", "0", "revision"], 1),
                    "in_group": safe_get(fact, ["general", "pulse_info", "pulses", "0", "in_group"], 0),
                    "is_subscribing": safe_get(fact, ["general", "pulse_info", "pulses", "0", "is_subscribing"], 0),
                    "malware_family": safe_get(fact, ["general", "pulse_info", "related", "other", "malware_families", "0"], r"%23Exploit:NtQueryIntervalProfile"),
                    "ip": safe_get(fact, ["passive_dns", "passive_dns", "0", "address"], "104.37.86.39"),
                    "source_city": safe_get(fact, ["geo", "city"], "Traverse City"),
                    "source_country": safe_get(fact, ["geo", "country_name"], "United States of America"),
                    "source_latitude": safe_get(fact, ["geo", "latitude"], 44.773289),
                    "source_longitude": safe_get(fact, ["geo", "longitude"], -85.701233),
                    "target_country": safe_get(fact, ["general", "pulse_info", "targeted_countries", "0"], "Croatia"),
                    "target_latitude": safe_get(fact, ["geo", "latitude"], 45.875832),
                    "target_longitude": safe_get(fact, ["geo", "longitude"], 15.85361),
                }

                # Send each fact to Kafka immediately
                try:
                    producer.send(KAFKA_INDICATORS_TOPIC, value=formatted_fact)
                    logger.info("Sent fact to Kafka: %s", formatted_fact)
                except Exception as e:
                    logger.error("Error sending fact to Kafka: %s", e)

        except Exception as e:
            logger.error("Failed to process indicator: %s", e)

# Kafka Producer Initialization
def initialize_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

# Produce Data to Kafka
def produce_data_to_kafka():
    producer = initialize_kafka_producer()
    while True:
        logger.info("Starting data ingestion process...")
        pulses = fetch_pulses()
        for pulse in pulses:
            pulse_id = pulse.get("id")
            if not pulse_id:
                logger.warning("Pulse does not contain an ID. Skipping.")
                continue
            indicators = fetch_indicators_for_pulse(pulse_id)
            fetch_facts_for_indicators(indicators, producer)
        time.sleep(5)

if __name__ == "__main__":
    # Verify Kafka topics and create if not exist
    check_and_create_kafka_topic(KAFKA_INDICATORS_TOPIC)
    produce_data_to_kafka()
