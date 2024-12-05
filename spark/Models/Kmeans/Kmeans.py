import pickle
from river import stream
import pandas as pd
from sklearn.preprocessing import LabelEncoder
import logging

class Kmeans:
    def __init__(self):
        # Load the pre-trained KMeans model
        self.kmeans_model = pickle.load(open('C:/Users/I745988/Ransomware-attack/spark/Models/Kmeans/Kmeans_4.pkl', 'rb')) 
    
    @staticmethod
    def preprocess_clustering_data(df):
        """Preprocess data for clustering (convert categorical features to integers)."""
        logging.info("Preprocessing data for clustering...")

        label_encoder = LabelEncoder()

        # Encode categorical columns for clustering
        categorical_columns = ['source_country', 'target_country', 'malware_family']
        for column in categorical_columns:
            if column in df.columns:
                logging.info(f"Encoding column: {column}")
                df[column] = label_encoder.fit_transform(df[column].astype(str))

        # Select features for clustering
        X = df[['source_country', 'target_country', 'malware_family']]
        return X
    
    def model(self, df):
            """Generate cluster labels for the input DataFrame."""
            logging.info("Starting clustering...")
            df = self.preprocess_clustering_data(df)  # Preprocess data to ensure numeric input

            kmeans_labels = []
            for x, _ in stream.iter_pandas(df):
                # Ensure input is numeric and float for predict_one
                x = {key: float(value) for key, value in x.items()}
                label = self.kmeans_model.predict_one(x)
                kmeans_labels.append(label)

            logging.info("Clustering completed.")
            return pd.DataFrame(kmeans_labels, columns=["KMeans labels"])
