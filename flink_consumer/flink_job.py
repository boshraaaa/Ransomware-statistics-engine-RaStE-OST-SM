from pyflink.datastream import StreamExecutionEnvironment
from kafka_source import create_kafka_source
from influxdb_sink import create_influxdb_sink
from transformations import process_stream

def main():
    env = StreamExecutionEnvironment.get_execution_environment()

    # Set up Kafka source
    kafka_source = create_kafka_source(env)

    # Process the data stream
    processed_stream = process_stream(kafka_source)

    # Set up InfluxDB sink
    influxdb_sink = create_influxdb_sink()

    # Add the sink to the stream
    processed_stream.add_sink(influxdb_sink)

    # Execute the Flink job
    env.execute("Flink Kafka Consumer")

if __name__ == "__main__":
    main()
