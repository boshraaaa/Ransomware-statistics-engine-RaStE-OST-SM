from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import ProcessFunction
from pyflink.datastream.state import ValueStateDescriptor
import json
from sklearn.preprocessing import LabelEncoder, MinMaxScaler
import numpy as np
import pandas as pd
from influxdb_client import InfluxDBClient, Point, WriteOptions


class InfluxDBProcessFunction(ProcessFunction):
    def __init__(self):
        # Initialize InfluxDB Client and Write API
        self.influx_client = InfluxDBClient(url="http://localhost:8086", token="your_token_here", org="ransomeware")
        self.write_api = self.influx_client.write_api(write_options=WriteOptions(batch_size=500, flush_interval=1000, jitter_interval=200))

    def process_element(self, value, ctx):
        try:
            # Assuming `value` is a dictionary-like object
            point = Point("processed_data") \
                .tag("indicator_id", value['indicator_id']) \
                .tag("indicator_type", value['indicator_type']) \
                .field("location_latitude", value['location_latitude']) \
                .field("location_longitude", value['location_longitude'])
            
            # Write the point to InfluxDB
            self.write_api.write(bucket="ransomeware", org="ransomeware", record=point)

            # Log for debugging
            print(f"Successfully wrote data to InfluxDB: {value}")

        except Exception as e:
            print(f"Error writing to InfluxDB: {e}")


def main():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()

    # Add the Kafka connector JAR to the execution environment
    env.add_jars("file:///C:/Users/I745988/Ransomware-attack/flink-sql-connector-kafka_2.11-1.13.0.jar")

    # Kafka Consumer Configuration
    kafka_consumer = FlinkKafkaConsumer(
        topics='indicators_topic',
        deserialization_schema=SimpleStringSchema(),
        properties={
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'flink-consumer-group'
        }
    )

    kafka_stream = env.add_source(kafka_consumer)

    # Preprocessing Function
    def preprocess_data(raw_data):
        try:
            data = json.loads(raw_data)
            
            # Handle Missing Values
            for key, value in data.items():
                if value is None or value == "":
                    if isinstance(value, (int, float)):
                        data[key] = 0
                    else:
                        data[key] = "unknown"

            # Label Encoding for Categorical Variables
            categorical_columns = ['indicator_type', 'pulse_TLP', 'pulse_author_name', 'pulse_name', 'location_city', 'location_country']
            for col in categorical_columns:
                le = LabelEncoder()
                data[col] = le.fit_transform([data[col]])[0]

            # Normalize Numeric Columns
            numeric_columns = ['location_latitude', 'location_longitude']
            scaler = MinMaxScaler()
            scaled_values = scaler.fit_transform([[data[col] for col in numeric_columns]])
            for idx, col in enumerate(numeric_columns):
                data[col] = scaled_values[0][idx]

            return data
        except Exception as e:
            return None

    processed_stream = kafka_stream.map(preprocess_data, output_type=Types.PICKLED_BYTE_ARRAY()).filter(lambda x: x is not None)

    # Create an instance of the custom InfluxDB ProcessFunction
    influx_db_process_function = InfluxDBProcessFunction()

    # Add the custom InfluxDB process function to the stream
    processed_stream.process(influx_db_process_function)

    # Execute the Flink job
    env.execute("Flink Preprocessing Job")

if __name__ == "__main__":
    main()
