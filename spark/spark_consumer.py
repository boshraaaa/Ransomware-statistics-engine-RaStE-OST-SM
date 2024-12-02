from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *
import logging
from influxdb_client import InfluxDBClient, Point, BucketRetentionRules
import time
from datetime import datetime
import pandas as pd
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import pandas as pd
import socket
import struct
from sklearn.preprocessing import LabelEncoder, StandardScaler

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


# Function to convert IP to integer representation
def ip_to_int(ip):
    if ip is None:
        return None  # Return None if IP is missing or null
    try:
        return struct.unpack("!I", socket.inet_aton(ip))[0]
    except socket.error:
        return None

# Complete Preprocessing Function
def preprocess_data(df):
    # Step 1: Drop unnecessary columns
    columns_to_drop = [
        'content', 'title', 'description_indicator', 'expiration', 
        'is_active', 'in_group', 'is_subscribing', 
        'description_pulse', 'author_name', 'modified', 'public', 
        'adversary'
    ]
    df.drop(columns=columns_to_drop, inplace=True)
    
    # Step 2: Label Encoding for categorical columns
    label_encoder = LabelEncoder()
    categorical_columns = [
        'source_city', 'source_country', 'target_country', 
        'malware_family'
    ]
    
    for column in categorical_columns:
        if column in df.columns:
            # Label encode the column, ensuring to handle non-string data types
            df[column] = label_encoder.fit_transform(df[column].astype(str))

    # Step 3: Encode IP addresses to integer representation
    if 'ip' in df.columns:
        df['ip_encoded'] = df['ip'].apply(ip_to_int)

    # Step 4: Feature Engineering - Extract date and day of the week from 'created_indicator'
    df['date'] = pd.to_datetime(df['created_indicator'], errors='coerce')  # Extract date
    
    # Step 5: Normalize latitude and longitude columns (source and target)
    latitude_longitude_columns = [
        ('source_latitude', 'source_longitude'),
        ('target_latitude', 'target_longitude')
    ]
    
    scaler = StandardScaler()

    for lat_col, lon_col in latitude_longitude_columns:
        if lat_col in df.columns and lon_col in df.columns:
            df[[lat_col, lon_col]] = scaler.fit_transform(df[[lat_col, lon_col]])

    return df

# Kafka Stream Schema
schema = StructType([
    StructField('id_indicator', LongType(), True),
    StructField('indicator', StringType(), True),
    StructField('type', StringType(), True),
    StructField('created_indicator', StringType(), True),
    StructField('content', StringType(), True),
    StructField('title', StringType(), True),
    StructField('description_indicator', StringType(), True),
    StructField('expiration', StringType(), True),
    StructField('is_active', IntegerType(), True),
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
    StructField('malware_family', StringType(), True),
    StructField('ip', StringType(), True),
    StructField('source_city', StringType(), True),
    StructField('source_country', StringType(), True),
    StructField('source_latitude', DoubleType(), True),
    StructField('source_longitude', DoubleType(), True),
    StructField('target_country', StringType(), True),
    StructField('target_latitude', DoubleType(), True),
    StructField('target_longitude', DoubleType(), True),
])

def train_classification_model(df):
    logger.info("Training classification models for source country, target country, and malware family...")

    # Ensure 'created_indicator' is correctly converted to Unix timestamp (int64)
    df['created_indicator_timestamp'] = pd.to_datetime(df['created_indicator'], errors='coerce').astype('int64') / 10**9  # Convert to Unix timestamp in seconds

    # Use the timestamp in the feature set
    X = df[['source_latitude', 'source_longitude', 'target_latitude', 'target_longitude', 'created_indicator_timestamp']]
    y_source = df['source_country']
    y_target = df['target_country']
    y_malware = df['malware_family']
    
    # Train test split
    X_train, X_test, y_source_train, y_source_test = train_test_split(X, y_source, test_size=0.2, random_state=42)
    _, _, y_target_train, y_target_test = train_test_split(X, y_target, test_size=0.2, random_state=42)
    _, _, y_malware_train, y_malware_test = train_test_split(X, y_malware, test_size=0.2, random_state=42)
    
    # Random Forest Classifier for source country prediction
    rf_source = RandomForestClassifier(n_estimators=100, random_state=42)
    rf_source.fit(X_train, y_source_train)
    source_predictions = rf_source.predict(X_test)
    source_accuracy = accuracy_score(y_source_test, source_predictions)
    logger.info(f"Source country prediction accuracy: {source_accuracy * 100:.2f}%")

    # Random Forest Classifier for target country prediction
    rf_target = RandomForestClassifier(n_estimators=100, random_state=42)
    rf_target.fit(X_train, y_target_train)
    target_predictions = rf_target.predict(X_test)
    target_accuracy = accuracy_score(y_target_test, target_predictions)
    logger.info(f"Target country prediction accuracy: {target_accuracy * 100:.2f}%")

    # Random Forest Classifier for malware family prediction
    rf_malware = RandomForestClassifier(n_estimators=100, random_state=42)
    rf_malware.fit(X_train, y_malware_train)
    malware_predictions = rf_malware.predict(X_test)
    malware_accuracy = accuracy_score(y_malware_test, malware_predictions)
    logger.info(f"Malware family prediction accuracy: {malware_accuracy * 100:.2f}%")

    return rf_source, rf_target, rf_malware




def process_batch(batch_df, batch_id):
    logger.info(f"Processing batch: {batch_id}")
    try:
        if batch_df.count() > 0:
            # Convert Spark DataFrame to Pandas (for simplicity)
            pandas_df = batch_df.toPandas()

            # Preprocess the data
            pandas_df = preprocess_data(pandas_df)

            # Train the classification models
            rf_source, rf_target, rf_malware = train_classification_model(pandas_df)

            # Predict on new data
            source_predictions = rf_source.predict(pandas_df[['source_latitude', 'source_longitude', 'target_latitude', 'target_longitude', 'created_indicator_timestamp']])
            target_predictions = rf_target.predict(pandas_df[['source_latitude', 'source_longitude', 'target_latitude', 'target_longitude', 'created_indicator_timestamp']])
            malware_predictions = rf_malware.predict(pandas_df[['source_latitude', 'source_longitude', 'target_latitude', 'target_longitude', 'created_indicator_timestamp']])

            # Write Predicted Results to InfluxDB
            for i, record in pandas_df.iterrows():
                created_indicator = int(time.time()*1e9)
                point = Point("indicator_data").tag("source_country", source_predictions[i]) \
                    .tag("target_country", target_predictions[i]) \
                    .tag("malware_family", malware_predictions[i]) \
                    .time(created_indicator)
                write_with_retry(write_api, point)
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
