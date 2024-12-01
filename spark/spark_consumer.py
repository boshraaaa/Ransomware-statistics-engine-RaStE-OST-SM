from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from influxdb_client import InfluxDBClient
import logging
import json

# Logging Configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('KafkaToInflux')

# Kafka Configuration
KAFKA_BROKER = 'kafka:9092'  # Adjust to your Kafka service name in Docker
KAFKA_INDICATORS_TOPIC = 'indicators_topic'

# InfluxDB Configuration
INFLUXDB_HOST = 'http://influxdb:8086'  # InfluxDB container hostname
AUTH_TOKEN = 'cnPhRh4jqqJgyAZciIP-Ulzkx_X76VqbK2MApufABa_WF_1Bdk0j3wOmFEorb2gOFGFF4ulMqsbOGMIWuIppQg=='
DEFAULT_BUCKET = 'ransomeware'
DEFAULT_ORGANIZATION = 'ransomeware'

# Initialize InfluxDB client
influx_client = InfluxDBClient(url=INFLUXDB_HOST, token=AUTH_TOKEN, org=DEFAULT_ORGANIZATION)
write_api = influx_client.write_api()

# Initialize Spark session with Kafka support
spark = SparkSession.builder \
    .appName('KafkaToInflux') \
    .getOrCreate()

# Read data from Kafka
df_kafka = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_INDICATORS_TOPIC) \
    .load()

# Parse the JSON data from Kafka messages (assuming that the value is JSON encoded)
df_parsed = df_kafka.selectExpr("CAST(value AS STRING) as json_data")

# Parse the JSON data and expand it into separate columns
df_json = df_parsed.select(
    expr("json_data['indicator_id'] as indicator_id"),
    expr("json_data['pulse_id'] as pulse_id"),
    expr("json_data['indicator'] as indicator"),
    expr("json_data['indicator_type'] as indicator_type"),
    expr("json_data['indicator_created'] as indicator_created"),
    expr("json_data['indicator_title'] as indicator_title"),
    expr("json_data['indicator_description'] as indicator_description"),
    expr("json_data['indicator_expiration'] as indicator_expiration"),
    expr("json_data['indicator_is_active'] as indicator_is_active"),
    expr("json_data['pulse_name'] as pulse_name"),
    expr("json_data['pulse_description'] as pulse_description"),
    expr("json_data['pulse_author_name'] as pulse_author_name"),
    expr("json_data['pulse_modified'] as pulse_modified"),
    expr("json_data['pulse_created'] as pulse_created"),
    expr("json_data['pulse_public'] as pulse_public"),
    expr("json_data['pulse_adversary'] as pulse_adversary"),
    expr("json_data['pulse_TLP'] as pulse_TLP"),
    expr("json_data['pulse_revision'] as pulse_revision"),
    expr("json_data['pulse_in_group'] as pulse_in_group"),
    expr("json_data['pulse_is_subscribing'] as pulse_is_subscribing"),
    expr("json_data['pulse_malware_family'] as pulse_malware_family"),
    expr("json_data['location_city'] as location_city"),
    expr("json_data['location_country'] as location_country"),
    expr("json_data['location_latitude'] as location_latitude"),
    expr("json_data['location_longitude'] as location_longitude")
)

# Display the stream schema and data (this is for debugging purposes)
df_json.printSchema()
df_json.show(truncate=False, n=5)

# Define a function to write data to InfluxDB
def write_to_influx(df, epoch_id):
    # Log that data is being written to InfluxDB
    logger.info(f"Writing data to InfluxDB (Epoch ID: {epoch_id})")
    
    # Convert DataFrame to Pandas and write to InfluxDB (you can adjust as needed)
    records = df.collect()  # Collect the data from the stream
    for record in records:
        point = {
            "measurement": "indicator_data",
            "tags": {
                "indicator_id": record['indicator_id'],
                "pulse_id": record['pulse_id'],
            },
            "fields": {
                "indicator": record['indicator'],
                "indicator_type": record['indicator_type'],
                "indicator_created": record['indicator_created'],
                "indicator_title": record['indicator_title'],
                "indicator_description": record['indicator_description'],
                "indicator_expiration": record['indicator_expiration'],
                "indicator_is_active": record['indicator_is_active'],
                "pulse_name": record['pulse_name'],
                "pulse_description": record['pulse_description'],
                "pulse_author_name": record['pulse_author_name'],
                "pulse_modified": record['pulse_modified'],
                "pulse_created": record['pulse_created'],
                "pulse_public": record['pulse_public'],
                "pulse_adversary": record['pulse_adversary'],
                "pulse_TLP": record['pulse_TLP'],
                "pulse_revision": record['pulse_revision'],
                "pulse_in_group": record['pulse_in_group'],
                "pulse_is_subscribing": record['pulse_is_subscribing'],
                "pulse_malware_family": record['pulse_malware_family'],
                "location_city": record['location_city'],
                "location_country": record['location_country'],
                "location_latitude": record['location_latitude'],
                "location_longitude": record['location_longitude'],
            },
            "time": record['indicator_created']  # You can adjust the timestamp field if needed
        }
        logger.info(f"Writing point to InfluxDB: {point}")
        write_api.write(bucket=DEFAULT_BUCKET, org=DEFAULT_ORGANIZATION, record=point)

# Stream data and write it to InfluxDB, with logging enabled
query = df_json.writeStream \
    .foreachBatch(write_to_influx) \
    .outputMode("append") \
    .start()

# Wait for the termination of the stream
query.awaitTermination()  # Keep the stream running
