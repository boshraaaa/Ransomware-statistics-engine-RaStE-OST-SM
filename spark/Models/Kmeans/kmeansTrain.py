import pandas as pd
from river import cluster, stream
from sklearn.preprocessing import LabelEncoder
import logging
import matplotlib.pyplot as plt
import pickle
import numpy as np

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def preprocess_clustering_data(df):
    """Preprocess data for clustering (convert categorical features to integers)."""
    logging.info("Preprocessing data for clustering...")
    label_encoder = LabelEncoder()

    # Encode categorical columns for clustering
    categorical_columns = ['source_country', 'target_country', 'malware_family']
    df = df.copy()  # Avoid modifying the original DataFrame
    for column in categorical_columns:
        if column in df.columns:
            logging.info(f"Encoding column: {column}")
            df[column] = label_encoder.fit_transform(df[column].astype(str))

    # Ensure all columns are numeric
    numeric_columns = ['source_country', 'target_country', 'malware_family']
    df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce')

    # Select features for clustering
    X = df[['source_country', 'target_country', 'malware_family']]
    return X

def train_clustering_model(df, n_clusters=4):
    """Train a River KMeans clustering model incrementally."""
    logging.info(f"Training River KMeans clustering model with {n_clusters} clusters...")
    
    # Initialize the River KMeans model
    kmeans = cluster.KMeans(n_clusters=n_clusters, halflife=0.5, sigma=3)
    
    # Stream the data and train the model incrementally
    for x, _ in stream.iter_pandas(df):
        kmeans.learn_one(x)
    
    # Save the trained model
    model_path = f'C:/Users/I745988/Ransomware-attack/spark/Models/Kmeans/Kmeans_{n_clusters}.pkl'
    with open(model_path, 'wb') as f:
        pickle.dump(kmeans, f)
    
    logging.info(f"River KMeans clustering model trained and saved to {model_path}.")
    return kmeans


def evaluate_clustering(df, max_k=8):
    """Evaluate clustering performance for different values of k using manual inertia computation."""
    inertia_values = []
    optimal_k = 0
    min_inertia = float('inf')

    logging.info("Evaluating clustering performance for different k values...")

    for k in range(2, max_k + 1):
        logging.info(f"Training KMeans for k={k}...")

        # Initialize the River KMeans model
        kmeans = cluster.KMeans(n_clusters=k, halflife=0.5, sigma=3)

        # Stream the data and train the model incrementally
        total_inertia = 0
        for x, _ in stream.iter_pandas(df):
            # Learn and calculate inertia manually
            kmeans.learn_one(x)
            centroid = kmeans.centers[kmeans.predict_one(x)]  # Get the nearest centroid
            total_inertia += np.sum((np.array(list(x.values())) - np.array(list(centroid.values())))**2)
        
        inertia_values.append(total_inertia)
        logging.info(f"Inertia for k={k}: {total_inertia}")

        # Track the optimal k based on the minimum inertia
        if total_inertia < min_inertia:
            min_inertia = total_inertia
            optimal_k = k

    # Plot the inertia values for the Elbow Method
    plot_inertia(inertia_values)
    return optimal_k

def plot_inertia(inertia_values):
    """Plot the Elbow Method using inertia values."""
    plt.figure(figsize=(8, 5))
    plt.plot(range(2, len(inertia_values) + 2), inertia_values, 'bx-')
    plt.xlabel('Number of Clusters (k)')
    plt.ylabel('Inertia')
    plt.title('Elbow Method For Optimal k')
    plt.show()



def export_predictions(df, kmeans_model):
    """Generate and export predictions for the dataset."""
    logging.info("Generating predictions using the trained River KMeans model...")
    cluster_labels = []

    # Preprocess the DataFrame to ensure numeric inputs
    processed_df = preprocess_clustering_data(df)

    # Stream the data and predict cluster labels
    for x, _ in stream.iter_pandas(processed_df):
        # Convert the feature dictionary to numeric values
        x = {key: float(value) for key, value in x.items()}  # Ensure all values are floats
        cluster_labels.append(kmeans_model.predict_one(x))

    # Add cluster labels to the original dataframe
    df['cluster'] = cluster_labels

    # Export to CSV
    output_path = 'C:/Users/I745988/Ransomware-attack/spark/Models/Kmeans/clustered_data.csv'
    df.to_csv(output_path, index=False)
    logging.info(f"Predictions exported to {output_path}")

if __name__ == "__main__":
    # Load data
    logging.info("Loading data...")
    df = pd.read_csv(
        'C:/Users/I745988/Ransomware-attack/data/enriched_data.csv',
        encoding='utf-8',
        low_memory=False
    )
    sub_df = preprocess_clustering_data(df.head(30000))  # Use a subset for testing

    # Evaluate clustering performance and determine the optimal k
    optimal_k = evaluate_clustering(sub_df, max_k=8)
    logging.info(f"The optimal number of clusters is: {optimal_k}")

    # Train the clustering model with the optimal k
    kmeans_model = train_clustering_model(sub_df, n_clusters=4)

    # Export predictions to a CSV file
    export_predictions(df.head(30000), kmeans_model)
