from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *
import logging
from influxdb_client import InfluxDBClient, Point, BucketRetentionRules
import time
from datetime import datetime
import pandas as pd
from sklearn.preprocessing import LabelEncoder, StandardScaler
import numpy as np

# Logging Configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('KafkaToInflux')

# Kafka Configuration
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'indicators_topic'

# InfluxDB Configuration
INFLUXDB_HOST = 'http://localhost:8086'
AUTH_TOKEN = 'lideTUM3U3sZwpmzk2sDbRayZ7zHKfZzo4U42doc24UZBzPznA1fjU-CeCxEglGuMnvYigYKLNo5S9oyvbjQsA==' 
DEFAULT_BUCKET = 'test111'
DEFAULT_ORGANIZATION = 'ransomeware'

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaToInflux") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
    .config("spark.network.timeout", "600s") \
    .config("spark.executor.heartbeatInterval", "100s") \
    .getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

# Initialize InfluxDB Client
influx_client = InfluxDBClient(url=INFLUXDB_HOST, token=AUTH_TOKEN, org=DEFAULT_ORGANIZATION)
write_api = influx_client.write_api()

# Ensure Bucket Exists
def ensure_bucket_exists():
    try:
        buckets = influx_client.buckets_api().find_buckets().buckets
        bucket_exists = False
        for bucket in buckets:
            if bucket.name == DEFAULT_BUCKET:
                bucket_exists = True
                logger.info(f"Bucket '{DEFAULT_BUCKET}' already exists.")
                break

        if not bucket_exists:
            logger.info(f"Bucket '{DEFAULT_BUCKET}' not found. Creating it...")
            influx_client.buckets_api().create_bucket(
                bucket_name=DEFAULT_BUCKET,
                org=DEFAULT_ORGANIZATION,
                retention_rules=[BucketRetentionRules(type="expire", every_seconds=0)]  # Never delete data
            )
            logger.info(f"Bucket '{DEFAULT_BUCKET}' created successfully.")
    except Exception as e:
        logger.error(f"Error checking/creating bucket: {e}")

# Retry logic for writing to InfluxDB
def write_with_retry(write_api, point, retries=3, delay=5):
    for attempt in range(retries):
        try:
            write_api.write(bucket=DEFAULT_BUCKET, org=DEFAULT_ORGANIZATION, record=point)
            logger.info(f"Record written to InfluxDB: {point}")
            return
        except Exception as e:
            logger.error(f"Attempt {attempt + 1} failed: {e}")
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                logger.error(f"Failed to write record after {retries} attempts: {e}")

# Preprocessing Function
# Preprocessing Function
def preprocess_data(df):
    # Step 1: Rename 'id_x' to 'id_indicator', 'created_x' to 'created_indicator', and 'created_y' to 'created_pulse'
    df.rename(columns={"countryName" : "countryName_encoded", "malware_family" : "malware_family_encoded", "TLP" : "TLP_encoded", 'id_x': 'id_indicator', 'created_x': 'created_indicator', 'created_y': 'created_pulse'}, inplace=True)
    
    # Step 2: Drop specified columns
    features_k = ['countryName_encoded', 'latitude', 'longitude', 'malware_family_encoded', 'TLP_encoded']
    columns_to_drop = [col for col in df.columns if col not in features_k]
    df.drop(columns=columns_to_drop, inplace=True)
    for feature in features_k:
        if feature not in df.columns:
            df[feature] = 0  # Add the missing feature with 0 as its value


    # Step 3: Fill missing values with random values from each respective row (independent for each column)
    for column in df.columns:
        if df[column].isnull().any():  # Only fill if there are missing values in the column
            # For each row with a missing value, fill with a random value from the same column's non-null values
            non_null_values = df[column].dropna().values  # Get all non-null values from the column
            df[column] = df[column].apply(lambda x: np.random.choice(non_null_values) if pd.isnull(x) else x)

    # Step 4: Encode categorical columns (Example encoding, adapt as necessary)
    label_encoder = LabelEncoder()
    df['countryName_encoded'] = label_encoder.fit_transform(df['countryName_encoded'])
    df['malware_family_encoded'] = label_encoder.fit_transform(df['malware_family_encoded'])
    df['TLP_encoded'] = label_encoder.fit_transform(df['TLP_encoded'])

    # Step 5: Normalize latitude and longitude columns
    if 'latitude' in df.columns and 'longitude' in df.columns:
        scaler = StandardScaler()
        df[['latitude', 'longitude']] = scaler.fit_transform(df[['latitude', 'longitude']])

    return df

# Kafka Stream Schema
schema = StructType([
    StructField('id_indicator', LongType(), True),
    StructField('pulse_id', StringType(), True),
    StructField('indicator', StringType(), True),
    StructField('type', StringType(), True),
    StructField('created_indicator', StringType(), True),
    StructField('content', StringType(), True),
    StructField('title', StringType(), True),
    StructField('description_indicator', StringType(), True),
    StructField('expiration', StringType(), True),
    StructField('is_active', IntegerType(), True),
    StructField('ip', StringType(), True),
    StructField('cityName', StringType(), True),
    StructField('countryName', StringType(), True),
    StructField('latitude', DoubleType(), True),
    StructField('longitude', DoubleType(), True),
    StructField('id_pulse', StringType(), True),
    StructField('name', StringType(), True),
    StructField('description_pulse', StringType(), True),
    StructField('author_name', StringType(), True),
    StructField('modified', StringType(), True),
    StructField('created_pulse', StringType(), True),
    StructField('public', IntegerType(), True),
    StructField('adversary', StringType(), True),
    StructField('TLP', StringType(), True),
    StructField('revision', IntegerType(), True),
    StructField('in_group', IntegerType(), True),
    StructField('is_subscribing', StringType(), True),
    StructField('malware_family', StringType(), True)
])

# Process Each Batch
def process_batch(batch_df, batch_id):
    logger.info(f"Processing batch: {batch_id}")
    try:
        if batch_df.count() > 0:
            # Convert Spark DataFrame to Pandas (for simplicity)
            pandas_df = batch_df.toPandas()

            # Preprocess the data
            pandas_df = preprocess_data(pandas_df)

            # Write Preprocessed Data to InfluxDB
            for record in pandas_df.to_dict(orient='records'):
                try:
                    # Ensure timestamp is correctly formatted
                    created_indicator = int(time.time()*1e9)
                    """indicator_created = record['created_indicator']

                    # Convert indicator_created to timestamp (nanoseconds)
                    try:
                        indicator_timestamp = int(datetime.strptime(indicator_created, '%Y-%m-%dT%H:%M:%S').timestamp() * 1e9)  # Convert to nanoseconds
                    except ValueError:
                        logger.error(f"Invalid timestamp format for record: {record['id_indicator']}")
                        continue  # Skip invalid timestamps"""
                    
                    # Create a Point for each field and tag in the record
                    point = Point("indicator_data").tag("countryName_encoded", record['countryName_encoded']) \
                        .tag("malware_family_encoded", record['malware_family_encoded']) \
                        .field("TLP_encoded", record['TLP_encoded']) \
                        .field("latitude", record['latitude']) \
                        .field("longitude", record['longitude']) \
                        .time(created_indicator)  # Ensure this timestamp is in nanoseconds
                    write_with_retry(write_api, point)
                except Exception as e:
                    logger.error(f"Failed to write record to InfluxDB: {e}")
        else:
            logger.info("No data in this batch.")
    except Exception as e:
        logger.error(f"Error processing batch {batch_id}: {e}")

# Ensure Bucket Exists Before Starting Stream
ensure_bucket_exists()

# Kafka Input Stream
kafka_stream = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON Data from Kafka
parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")).select("data.*")

# Write Stream Data to InfluxDB in Micro-Batches
query = parsed_stream.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()