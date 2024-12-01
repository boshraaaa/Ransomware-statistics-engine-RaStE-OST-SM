# File: flink_clustering_job.py
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.typeinfo import Types
from sklearn.cluster import MiniBatchKMeans
import json
import numpy as np

# File: flink_clustering_job.py
import os
import pickle
from sklearn.cluster import MiniBatchKMeans

MODEL_PATH = "/app/models/clustering_model.pkl"

# Load existing model or create a new one
if os.path.exists(MODEL_PATH):
    with open(MODEL_PATH, "rb") as f:
        model = pickle.load(f)
else:
    model = MiniBatchKMeans(n_clusters=5, batch_size=10)

# Save model periodically
def save_model():
    with open(MODEL_PATH, "wb") as f:
        pickle.dump(model, f)
    print("Model saved.")

# Ensure to call save_model() at the end of the clustering process


def main():
    env = StreamExecutionEnvironment.get_execution_environment()

    kafka_consumer = FlinkKafkaConsumer(
        topics='indicators_topic',
        deserialization_schema=SimpleStringSchema(),
        properties={
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'flink-clustering-group'
        }
    )

    kafka_stream = env.add_source(kafka_consumer)

    # Clustering Function
    def cluster_data(raw_data):
        try:
            data = json.loads(raw_data)
            feature_vector = np.array([
                data['location_latitude'],
                data['location_longitude'],
                data['indicator_type']
            ]).reshape(1, -1)

            # Apply MiniBatchKMeans
            model = MiniBatchKMeans(n_clusters=5, batch_size=10)
            model.partial_fit(feature_vector)
            cluster_label = model.predict(feature_vector)[0]

            # Append cluster label to data
            data['cluster_label'] = int(cluster_label)
            return json.dumps(data)
        except Exception as e:
            return None

    clustered_stream = kafka_stream.map(cluster_data, output_type=Types.STRING()).filter(lambda x: x is not None)

    clustered_stream.print()  # Replace with a sink for production
    save_model()
    env.execute("Flink Clustering Job")

if __name__ == "__main__":
    main()
