from confluent_kafka import Producer
import sqlite3
import json
import time

# Kafka Producer Initialization
def create_kafka_producer(bootstrap_servers):
    return Producer({'bootstrap.servers': bootstrap_servers})

# Send data to Kafka
def send_to_kafka(producer, topic, key, value):
    producer.produce(topic, key=key, value=value)
    producer.flush()

# Fetch joined data from SQLite and stream to Kafka
def fetch_and_send_joined_data(db_file, producer, kafka_topic, chunk_size, interval_seconds):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # SQL Query to join tables
    join_query = """
    SELECT 
        ind.id AS indicator_id,
        ind.pulse_id,
        ind.indicator,
        ind.type,
        ind.created AS indicator_created,
        ind.content,
        ind.title AS indicator_title,
        ind.description AS indicator_description,
        ind.expiration,
        ind.is_active,
        loc.cityName,
        loc.countryName,
        loc.latitude,
        loc.longitude,
        puls.name AS pulse_name,
        puls.description AS pulse_description,
        puls.author_name,
        puls.malware_family
    FROM 
        indicators AS ind
    INNER JOIN 
        ip_location AS loc ON ind.indicator = loc.ip
    INNER JOIN 
        pulses AS puls ON ind.pulse_id = puls.id
    LIMIT ? OFFSET ?;
    """

    offset = 0
    while True:
        print(f"Fetching rows {offset} to {offset + chunk_size} from database...")
        cursor.execute(join_query, (chunk_size, offset))
        rows = cursor.fetchall()

        if not rows:
            print("No more rows to fetch. Exiting.")
            break

        # Get column names for JSON serialization
        column_names = [description[0] for description in cursor.description]
        serialized_rows = [dict(zip(column_names, row)) for row in rows]

        # Send serialized rows to Kafka
        for row in serialized_rows:
            send_to_kafka(producer, kafka_topic, key=None, value=json.dumps(row))

        print(f"Sent {len(rows)} rows to Kafka.")
        offset += chunk_size
        time.sleep(interval_seconds)

    cursor.close()
    conn.close()


