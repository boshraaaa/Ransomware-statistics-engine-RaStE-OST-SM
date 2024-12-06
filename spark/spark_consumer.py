from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, desc
from pyspark.sql.types import *
from pyspark.sql.functions import to_timestamp
import logging
from pyspark.sql.functions import window, count
from influxdb_client import InfluxDBClient, Point, BucketRetentionRules
import time
from datetime import datetime
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import pandas as pd
import socket
import struct
import random
from sklearn.preprocessing import LabelEncoder, StandardScaler
import joblib 
from joblib import Parallel, delayed
from pyspark.sql.functions import window, count
from pyspark.sql.functions import to_date
from pyspark.sql.functions import month, year
from Models.Forcast.RandsomRegressor import RandomForestModel
from pyspark.sql.functions import to_date
from Models.Kmeans.Kmeans import Kmeans
from pyspark.sql.functions import col, desc, row_number
from pyspark.sql.functions import month, year
from pyspark.sql import functions as F


# Logging Configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('KafkaToInflux')

# Kafka Configuration
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'indicators_topic'

# InfluxDB Configuration
INFLUXDB_HOST = 'http://localhost:8086'
AUTH_TOKEN = 'JkLVh_Glxl0FfIHnJM3C8HZOVvY_kG_spqDAJ4yK2HlhH7ia6oQqLf5IOy2XpvzMVlThyoFVjiAfsztM_CE8vw==' 
DEFAULT_ORGANIZATION = 'ransomeware'
# Buckets
DEFAULT_BUCKET = 'ransomware' # for the 10 pipelines
PREDICTION_BUCKET = 'prediction' # for clustering
FORCAST_BUCKET = "forcast" # for forcast

#------------- Initialize Spark Session ---------------------------------#
spark = SparkSession.builder \
    .appName("KafkaToInflux") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
    .getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

#---------------- Initialize InfluxDB Client ------------------------#
influx_client = InfluxDBClient(url=INFLUXDB_HOST, token=AUTH_TOKEN, org=DEFAULT_ORGANIZATION)
write_api = influx_client.write_api()

#----------------------Retry logic for writing to InfluxDB----------------------------------#
def write_with_retry(write_api, point, bucket, retries=3, delay=5):
    for attempt in range(retries):
        try:
            logger.info(f"Attempting to write to bucket '{bucket}': {point}")
            write_api.write(bucket=bucket, org=DEFAULT_ORGANIZATION, record=point)
            logger.info(f"Record successfully written to bucket '{bucket}'.")
            return
        except Exception as e:
            logger.error(f"Attempt {attempt + 1} failed: {e}")
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                logger.error(f"Failed to write record after {retries} attempts.")


#----------------- Kafka Stream Schema ------------------#
schema = StructType([
    StructField('id_indicator', LongType(), True),
    StructField('indicator', StringType(), True),
    StructField('type', StringType(), True),
    StructField('created_indicator', TimestampType(), False),
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
    StructField('source_country', StringType(), False),
    StructField('source_latitude', DoubleType(), False),
    StructField('source_longitude', DoubleType(), False),
    StructField('target_country', StringType(), False),
    StructField('target_latitude', DoubleType(), False),
    StructField('target_longitude', DoubleType(), False),
])

# ------------Ensure Bucket Exists Before Starting Stream-----------------""
def ensure_bucket_exists(bucket_name):
    try:
        buckets = influx_client.buckets_api().find_buckets().buckets
        if not any(bucket.name == bucket_name for bucket in buckets):
            logger.info(f"Bucket '{bucket_name}' not found. Creating it...")
            influx_client.buckets_api().create_bucket(
                bucket_name=bucket_name,
                org=DEFAULT_ORGANIZATION,
                retention_rules=[BucketRetentionRules(type="expire", every_seconds=0)]  # Never delete data
            )
            logger.info(f"Bucket '{bucket_name}' created successfully.")
        else:
            logger.info(f"Bucket '{bucket_name}' already exists.")
    except Exception as e:
        logger.error(f"Error ensuring bucket '{bucket_name}': {e}")

# Ensure both buckets exist
ensure_bucket_exists(DEFAULT_BUCKET)
ensure_bucket_exists(PREDICTION_BUCKET)
ensure_bucket_exists(FORCAST_BUCKET)

#----------------Kafka Input Stream--------------------------------#
kafka_stream = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

#----------------------Parse JSON Data from Kafka-----------------------
parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")).select("data.*")
    
#--------------------Batch processing with pre-trained models-----------------------
K_means = Kmeans()

def process_clustering(batch_df):
    logger.info("==========================Clustering pipeline:===============================")
    try:
        if batch_df.isEmpty():
            logger.info("No data in this batch.")
            return
        
        rows = batch_df.collect()  # Collect the rows as a list
        pandas_df = pd.DataFrame(rows, columns=batch_df.columns) 
        logger.info("Initial Pandas DataFrame:")
        logger.info(f"\n{pandas_df}")

        # Validate required columns
        required_columns = ['source_country', 'target_country', 'malware_family']
        missing_columns = [col for col in required_columns if col not in pandas_df.columns]
        if missing_columns:
            logger.error(f"Missing required columns: {missing_columns}")
            return

        # Preprocess data
        pandas_df['created_indicator'] = pd.to_datetime(
            pandas_df['created_indicator'], errors='coerce', format='%Y-%m-%d %H:%M:%S'
        )
        X = Kmeans.preprocess_clustering_data(pandas_df)

        # Generate cluster labels
        kmeans_labels = K_means.model(X)
        pandas_df['cluster'] = kmeans_labels['KMeans labels']

        # Write predictions to InfluxDB
        for _, record in pandas_df.iterrows():
            point = Point("indicator_predictions") \
                .tag("id_indicator", str(record['id_indicator'])) \
                .tag("indicator", record['indicator']) \
                .tag("type", record['type']) \
                .tag("title", record['title']) \
                .tag("description_indicator", record['description_indicator']) \
                .tag("is_active", str(record['is_active'])) \
                .tag("id_pulse", record['id_pulse']) \
                .tag("name", record['name']) \
                .tag("author_name", record['author_name']) \
                .tag("TLP", record['TLP']) \
                .tag("malware_family", record['malware_family']) \
                .tag("ip", record['ip']) \
                .tag("source_city", record['source_city']) \
                .tag("source_country", record['source_country']) \
                .tag("target_country", record['target_country']) \
                .tag("cluster", str(record['cluster'])) \
                .field("source_latitude", record['source_latitude']) \
                .field("source_longitude", record['source_longitude']) \
                .field("target_latitude", record['target_latitude']) \
                .field("target_longitude", record['target_longitude']) \
                .time(record['created_indicator'])
            write_with_retry(write_api, point, bucket=PREDICTION_BUCKET)

    except Exception as e:
        logger.error(f"Error processing batch: {e}")

#-----------------------Load the pre-trained RandomForestModel---------------------------#
rf_model = RandomForestModel(model_path="C:/Users/I745988/Ransomware-attack/spark/Models/Forcast/Forcastrandom_forest_model.pkl")  
     
def process_batch(batch_df):
    logger.info("============== Processing Batch ==============")
    try:
        if batch_df.isEmpty():
            logger.info("No data in this batch.")
            return
        # Convert Spark DataFrame to Pandas
        rows = batch_df.collect()
        pandas_df = pd.DataFrame(rows, columns=batch_df.columns)
        # Log initial DataFrame
        logger.info("Initial Pandas DataFrame:")
        logger.info(f"\n{pandas_df.columns}")
        # Preprocess the DataFrame
        pandas_df = rf_model.preprocess(pandas_df)
        # Predict
        logger.info("--------------- Starting predictions ----------------")
        predictions = rf_model.predict(pandas_df)
        logger.info(f"Predictions completed. Predictions shape: {predictions.shape}")
        logger.info(f"Prediction output DataFrame:\n{predictions.columns}")
        # Write to InfluxDB
        for _, record in predictions.iterrows():
            try:
                point = Point("indicator_predictions") \
                    .field("num_attacks", record['num_attacks']) \
                    .time(record['created_indicator'])
                write_with_retry(write_api, point, bucket=FORCAST_BUCKET)
            except Exception as write_error:
                logger.error(f"Error writing record to InfluxDB: {write_error}")

    except Exception as e:
        logger.error(f"Error processing batch: {e}")

########################-----------------PIPLINES------------------####################################
def pipeline_top_10_targets_per_country(batch_df):
    batch_df = batch_df.filter(col("target_country").isNotNull())
    
    # Group by target_country and indicator, and calculate attack count
    result = batch_df.groupBy("target_country", "indicator") \
        .count() \
        .withColumnRenamed("count", "attack_count")
    
    # Sort by target_country and attack_count in descending order
    result_sorted = result.orderBy("target_country", desc("attack_count"))
    
    # Remove duplicate countries, keeping only the top attack count per country
    from pyspark.sql.window import Window
    windowSpec = Window.partitionBy("target_country").orderBy(desc("attack_count"))
    result_top_1_per_country = result_sorted.withColumn("rank", row_number().over(windowSpec)) \
                                             .filter(col("rank") == 1) \
                                             .drop("rank")
    
    result_top_1_per_country.show()  # Replace with writing to a sink if required

    # Write to InfluxDB
    for row in result_top_1_per_country.collect():
        point = Point("top_10_target_countries") \
            .tag("target_country", row["target_country"]) \
            .tag("indicator", row["indicator"]) \
            .field("attack_count", row["attack_count"])
        write_with_retry(write_api, point, bucket=DEFAULT_BUCKET)


def pipeline_top_10_threat_sources(batch_df):
    result = batch_df.groupBy("source_country") \
        .count() \
        .withColumnRenamed("count", "attack_count") \
        .orderBy(desc("attack_count")) \
        .limit(10)
    result.show()  # Replace with writing to a sink if required

    # Write to InfluxDB
    for row in result.collect():
        point = Point("top_10_source_countries") \
            .tag("source_country", row["source_country"]) \
            .field("attack_count", row["attack_count"])
        write_with_retry(write_api, point, bucket=DEFAULT_BUCKET)

def pipeline_top_10_active_ips(batch_df):
    batch_df = batch_df.filter(col("ip").isNotNull())
    result = batch_df.groupBy("ip") \
        .count() \
        .withColumnRenamed("count", "attack_count") \
        .orderBy(desc("attack_count")) \
        .limit(10)
    result.show() 

    # Write to InfluxDB
    for row in result.collect():
        point = Point("top_10_active_ips") \
            .tag("ip", row["ip"]) \
            .field("attack_count", row["attack_count"])
        write_with_retry(write_api, point, bucket=DEFAULT_BUCKET)

def pipeline_top_attack_type(batch_df):
    batch_df = batch_df.filter(col("type").isNotNull())
    result = batch_df.groupBy("type") \
        .count() \
        .withColumnRenamed("count", "attack_count") \
        .orderBy(desc("attack_count")) \
        .limit(1) 
    result.show() 

    # Write to InfluxDB
    for row in result.collect():
        point = Point("top_attack_type") \
            .tag("type", row["type"]) \
            .field("attack_count", row["attack_count"])
        write_with_retry(write_api, point, bucket=DEFAULT_BUCKET)

def pipeline_detect_target_country_changes(batch_df):
    batch_df = batch_df.filter(col("target_country").isNotNull())
    result = batch_df \
        .groupBy(
            "target_country",
            window(col("created_indicator"), "24 hours")
        ) \
        .agg(count("*").alias("count")) 
    sorted_stream = result.orderBy("window", ascending=True)
    sorted_stream.show() 

    # Write to InfluxDB
    for row in sorted_stream.collect():
        point = Point("detect_target_country_changes") \
            .tag("target_country", row["target_country"]) \
            .tag("window", str(row["window"])) \
            .field("count", row["count"])
        write_with_retry(write_api, point, bucket=DEFAULT_BUCKET)

def pipeline_detect_source_country_changes(batch_df):
    batch_df = batch_df.filter(col("source_country").isNotNull())
    result = batch_df \
        .groupBy(
            "source_country",
            window(col("created_indicator"), "24 hours") 
        ) \
        .agg(count("*").alias("attack_count")) 
    sorted_stream = result.orderBy("window", ascending=True)
    sorted_stream.show() 

    # Write to InfluxDB
    for row in sorted_stream.collect():
        point = Point("detect_source_country_changes") \
            .tag("source_country", row["source_country"]) \
            .tag("window", str(row["window"])) \
            .field("attack_count", row["attack_count"])
        write_with_retry(write_api, point, bucket=DEFAULT_BUCKET)

def pipeline_attacks_by_creation_day(batch_df):
    batch_df = batch_df.filter(col("created_indicator").isNotNull())
    result = batch_df.withColumn("created_date", to_date(col("created_indicator"))) \
        .groupBy("created_date") \
        .count() \
        .withColumnRenamed("count", "attack_count") \
        .orderBy("created_date", ascending=True)
    result_distinct = result.distinct()
    result_distinct.show()

    # Write to InfluxDB
    for row in result.collect():
        point = Point(measurement_name="attack_by_creation_day") \
            .tag("created_date", row["created_date"]) \
            .field("attack_count", row["attack_count"])
        write_with_retry(write_api, point, bucket=DEFAULT_BUCKET)


def pipeline_attacks_by_creation_month(batch_df):
    result = batch_df.withColumn("year", year(col("created_indicator"))) \
        .withColumn("month", month(col("created_indicator"))) \
        .groupBy("year", "month") \
        .count() \
        .withColumnRenamed("count", "attack_count") \
        .orderBy("year", "month", ascending=True)
    result_distinct = result.distinct()
    result_distinct.show()

    # Write to InfluxDB
    for row in result_distinct.collect():
        point = Point("attacks_by_creation_month") \
            .tag("year", str(row["year"])) \
            .tag("month", str(row["month"])) \
            .field("attack_count", row["attack_count"])
        write_with_retry(write_api, point, bucket=DEFAULT_BUCKET)

def pipeline_top_10_authors(batch_df):
    batch_df = batch_df.filter(col("author_name").isNotNull())
    result = batch_df.groupBy("author_name") \
        .count() \
        .withColumnRenamed("count", "indicator_count") \
        .orderBy(desc("indicator_count")) \
        .limit(10)
    result.show()

    # Write to InfluxDB
    for row in result.collect():
        point = Point("top_10_authors") \
            .tag("author_name", row["author_name"]) \
            .field("indicator_count", row["indicator_count"])
        write_with_retry(write_api, point, bucket=DEFAULT_BUCKET)
        
def pipeline_top_10_cities(batch_df):
    batch_df = batch_df.filter(col("source_city").isNotNull())
    result = batch_df.groupBy("source_city") \
        .count() \
        .withColumnRenamed("count", "attack_count") \
        .orderBy(desc("attack_count")) \
        .limit(10)
    result.show()

    # Write to InfluxDB
    for row in result.collect():
        point = Point("top_10_cities") \
            .tag("source_city", row["source_city"]) \
            .field("attack_count", row["attack_count"])
        write_with_retry(write_api, point, bucket=DEFAULT_BUCKET)


def top_10_pulse_tlp(batch_df):
    batch_df = batch_df.filter(F.col("TLP").isNotNull())
    result = batch_df.groupBy("TLP") \
        .count() \
        .withColumnRenamed("count", "tlp_count") \
        .orderBy(F.desc("tlp_count")) \
        .limit(10)
    result.show()

    # Write to InfluxDB
    for row in result.collect():
        point = Point("top_10_pulse_tlp") \
            .tag("TLP", row["TLP"]) \
            .field("tlp_count", row["tlp_count"])
        write_with_retry(write_api, point, bucket=DEFAULT_BUCKET)

def pipeline_top_10_threat_sources(batch_df):
    result = batch_df.groupBy("source_country") \
        .count() \
        .withColumnRenamed("count", "attack_count") \
        .orderBy(desc("attack_count")) \
        .limit(10)
    result.show()  # Replace with writing to a sink if required

    # Write to InfluxDB
    for row in result.collect():
        point = Point("top_10_source_countries") \
            .tag("source_country", row["source_country"]) \
            .field("attack_count", row["attack_count"])
        write_with_retry(write_api, point, bucket=DEFAULT_BUCKET)

def pipeline_top_10_active_ips(batch_df):
    batch_df = batch_df.filter(col("ip").isNotNull())
    result = batch_df.groupBy("ip") \
        .count() \
        .withColumnRenamed("count", "attack_count") \
        .orderBy(desc("attack_count")) \
        .limit(10)
    result.show() 

    # Write to InfluxDB
    for row in result.collect():
        point = Point("top_10_active_ips") \
            .tag("ip", row["ip"]) \
            .field("attack_count", row["attack_count"])
        write_with_retry(write_api, point, bucket=DEFAULT_BUCKET)

def pipeline_top_attack_type(batch_df):
    batch_df = batch_df.filter(col("type").isNotNull())
    result = batch_df.groupBy("type") \
        .count() \
        .withColumnRenamed("count", "attack_count") \
        .orderBy(desc("attack_count")) \
        .limit(1) 
    result.show() 

    # Write to InfluxDB
    for row in result.collect():
        point = Point("top_attack_type") \
            .tag("type", row["type"]) \
            .field("attack_count", row["attack_count"])
        write_with_retry(write_api, point, bucket=DEFAULT_BUCKET)

def pipeline_detect_target_country_changes(batch_df):
    batch_df = batch_df.filter(col("target_country").isNotNull())
    result = batch_df \
        .groupBy(
            "target_country",
            window(col("created_indicator"), "10 minutes")
        ) \
        .agg(count("*").alias("count")) 
    sorted_stream = result.orderBy("window", ascending=True)
    sorted_stream.show() 

    # Write to InfluxDB
    for row in sorted_stream.collect():
        point = Point("detect_target_country_changes") \
            .tag("target_country", row["target_country"]) \
            .tag("window", str(row["window"])) \
            .field("count", row["count"])
        write_with_retry(write_api, point, bucket=DEFAULT_BUCKET)

def pipeline_detect_source_country_changes(batch_df):
    batch_df = batch_df.filter(col("source_country").isNotNull())
    result = batch_df \
        .groupBy(
            "source_country",
            window(col("created_indicator"), "10 minutes") 
        ) \
        .agg(count("*").alias("attack_count")) 
    sorted_stream = result.orderBy("window", ascending=True)
    sorted_stream.show() 

    # Write to InfluxDB
    for row in sorted_stream.collect():
        point = Point("detect_source_country_changes") \
            .tag("source_country", row["source_country"]) \
            .tag("window", str(row["window"])) \
            .field("attack_count", row["attack_count"])
        write_with_retry(write_api, point, bucket=DEFAULT_BUCKET)

def pipeline_attacks_by_creation_day(batch_df):
    batch_df = batch_df.filter(col("created_indicator").isNotNull())
    result = batch_df.withColumn("created_date", to_date(col("created_indicator"))) \
        .groupBy("created_date") \
        .count() \
        .withColumnRenamed("count", "attack_count") \
        .orderBy("created_date", ascending=True)
    result_distinct = result.distinct()
    result_distinct.show()

    # Write to InfluxDB
    for row in result.collect():
        point = Point("attack_by_creation_day") \
            .tag("created_date", row["created_date"]) \
            .field("attack_count", row["attack_count"])
        write_with_retry(write_api, point, bucket=DEFAULT_BUCKET)


def pipeline_attacks_by_creation_month(batch_df):
    result = batch_df.withColumn("year", year(col("created_indicator"))) \
        .withColumn("month", month(col("created_indicator"))) \
        .groupBy("year", "month") \
        .count() \
        .withColumnRenamed("count", "attack_count") \
        .orderBy("year", "month", ascending=True)
    result_distinct = result.distinct()
    result_distinct.show()

    # Write to InfluxDB
    for row in result_distinct.collect():
        point = Point("attacks_by_creation_month") \
            .tag("year", str(row["year"])) \
            .tag("month", str(row["month"])) \
            .field("attack_count", row["attack_count"])
        write_with_retry(write_api, point, bucket=DEFAULT_BUCKET)


def pipeline_top_10_authors(batch_df):
    batch_df = batch_df.filter(col("author_name").isNotNull())
    result = batch_df.groupBy("author_name") \
        .count() \
        .withColumnRenamed("count", "indicator_count") \
        .orderBy(desc("indicator_count")) \
        .limit(10)
    result.show()

    # Write to InfluxDB
    for row in result.collect():
        point = Point("top_10_authors") \
            .tag("author_name", row["author_name"]) \
            .field("indicator_count", row["indicator_count"])
        write_with_retry(write_api, point, bucket=DEFAULT_BUCKET)
        
def pipeline_top_10_cities(batch_df):
    batch_df = batch_df.filter(col("source_city").isNotNull())
    result = batch_df.groupBy("source_city") \
        .count() \
        .withColumnRenamed("count", "attack_count") \
        .orderBy(desc("attack_count")) \
        .limit(10)
    result.show()

    # Write to InfluxDB
    for row in result.collect():
        point = Point("top_10_cities") \
            .tag("source_city", row["source_city"]) \
            .field("attack_count", row["attack_count"])
        write_with_retry(write_api, point, bucket=DEFAULT_BUCKET)


def top_10_pulse_tlp(batch_df):
    batch_df = batch_df.filter(F.col("TLP").isNotNull())
    result = batch_df.groupBy("TLP") \
        .count() \
        .withColumnRenamed("count", "tlp_count") \
        .orderBy(F.desc("tlp_count")) \
        .limit(10)
    result.show()

    # Write to InfluxDB
    for row in result.collect():
        point = Point("top_10_pulse_tlp") \
            .tag("TLP", row["TLP"]) \
            .field("tlp_count", row["tlp_count"])
        write_with_retry(write_api, point, bucket=DEFAULT_BUCKET)

def process_all_pipelines(batch_df, batch_id):
    print(f"§§§§§§§§§§§§§§§§§§§§§§§§§§§§§!! Processing batch !!§§§§§§§§§§§§§§§§§§§§§§§§§§§§§ {batch_id}")

    batch_df.cache()  # Cache the shared dataset to avoid redundant computation
    logger.info("==============PREDICTION==============================")
    process_batch(batch_df) #forcasting
    process_clustering(batch_df) #Clustering
    logger.info("Top 10 target countries")
    pipeline_top_10_targets_per_country(batch_df)
    logger.info("Top 10 threat sources")
    pipeline_top_10_threat_sources(batch_df)
    logger.info("Top 10 active IPs")
    pipeline_top_10_active_ips(batch_df)
    logger.info("Top Attac Type")
    pipeline_top_attack_type(batch_df)
    logger.info("Target country changes")
    pipeline_detect_target_country_changes(batch_df)
    logger.info("Source country changes")
    pipeline_detect_source_country_changes(batch_df)
    logger.info("Attacks by creation day")
    pipeline_attacks_by_creation_day(batch_df)
    logger.info("Attacks by creation month")
    pipeline_attacks_by_creation_month(batch_df)
    logger.info("Top 10 source cities")
    pipeline_top_10_cities(batch_df)
    logger.info("Top 10 authors")
    pipeline_top_10_authors(batch_df)

query = parsed_stream.writeStream \
    .foreachBatch(process_all_pipelines) \
    .outputMode("append") \
    .trigger(processingTime="10 minutes") \
    .start()

# Await all streams
query.awaitTermination()