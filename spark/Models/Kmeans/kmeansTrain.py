import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import LabelEncoder
import pickle
import logging
import time
import matplotlib.pyplot as plt
from sklearn.metrics import calinski_harabasz_score, davies_bouldin_score, silhouette_score

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def preprocess_clustering_data(df):
    """Preprocess data for clustering (convert categorical features to integers)"""
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

def train_clustering_model(df, n_clusters=4):
    """Train KMeans clustering model on the provided features"""
    logging.info("Training KMeans clustering model...")
    X = preprocess_clustering_data(df)
    
    # Train KMeans model
    kmeans = KMeans(n_clusters=n_clusters, random_state=42)
    kmeans.fit(X)
    
    # Save model
    model_path = f'C:/Users/I745988/Ransomware-attack/spark/Models/Kmeans/Kmeans_{n_clusters}.pkl'
    with open(model_path, 'wb') as f:
        pickle.dump(kmeans, f)
    
    logging.info(f"KMeans clustering model trained with {n_clusters} clusters.")
    return kmeans

def evaluate_clustering(X, max_k=8):
    """Evaluate clustering performance for different values of k"""
    Sum_of_squared_distances = []
    sil_scores = []
    ch_scores = []
    db_scores = []
    optimal_k = 0
    best_sil_score = -1  # To track the best silhouette score

    logging.info("Evaluating clustering performance for different k values...")

    for k in range(2, max_k + 1):
        tim_start = time.time()

        # Train KMeans model with k clusters
        kmeans = KMeans(n_clusters=k, random_state=42)
        kmeans.fit(X)
        logging.info("Train complete...")

        # Calculate metrics
        labels = kmeans.labels_
        logging.info("Calculating metrics...")

        # Inertia
        Sum_of_squared_distances.append(kmeans.inertia_)

        # Silhouette Score
        sil_score = silhouette_score(X, labels)
        sil_scores.append(sil_score)

        # Calinski-Harabasz Score
        ch_scores.append(calinski_harabasz_score(X, labels))

        # Davies-Bouldin Score
        db_scores.append(davies_bouldin_score(X, labels))

        logging.info(f"Metrics for k={k} calculated. Time taken: {time.time() - tim_start:.2f} seconds.")
        
        # Track the optimal k based on the best silhouette score
        if sil_score > best_sil_score:
            best_sil_score = sil_score
            optimal_k = k

    # Plot the results
    plot_clustering_metrics(Sum_of_squared_distances, sil_scores, ch_scores, db_scores)
    
    return optimal_k

def plot_clustering_metrics(Sum_of_squared_distances, sil_scores, ch_scores, db_scores):
    """Plot evaluation metrics"""
    plt.figure(figsize=(12, 6))
    
    # Elbow Method: Sum of Squared Distances
    plt.subplot(2, 2, 1)
    plt.plot(range(2, len(Sum_of_squared_distances) + 2), Sum_of_squared_distances, 'bx-')
    plt.xlabel('k')
    plt.ylabel('Sum_of_squared_distances')
    plt.title('Elbow Method For Optimal k')

    # Silhouette Score
    plt.subplot(2, 2, 2)
    plt.plot(range(2, len(sil_scores) + 2), sil_scores, 'bx-')
    plt.xlabel('k')
    plt.ylabel('Silhouette Score')
    plt.title('Silhouette Score for optimal k')

    # Calinski-Harabasz Score
    plt.subplot(2, 2, 3)
    plt.plot(range(2, len(ch_scores) + 2), ch_scores, 'bx-')
    plt.xlabel('k')
    plt.ylabel('Calinski-Harabasz Score')
    plt.title('Calinski-Harabasz Score for optimal k')

    # Davies-Bouldin Score
    plt.subplot(2, 2, 4)
    plt.plot(range(2, len(db_scores) + 2), db_scores, 'bx-')
    plt.xlabel('k')
    plt.ylabel('Davies-Bouldin Score')
    plt.title('Davies-Bouldin Score for optimal k')

    plt.tight_layout()
    plt.show()

def export_predictions(df, kmeans_model):
    """Generate and export predictions for the dataset."""
    logging.info("Generating predictions using the trained KMeans model...")
    X = preprocess_clustering_data(df)
    df['cluster'] = kmeans_model.predict(X)
    
    # Export to CSV
    output_path = 'C:/Users/I745988/Ransomware-attack/spark/Models/Kmeans/clustered_data.csv'
    df.to_csv(output_path, index=False)
    logging.info(f"Predictions exported to {output_path}")

if __name__ == "__main__":
    # Load data
    logging.info("Loading data...")
    df = pd.read_csv('C:/Users/I745988/Ransomware-attack/data/enriched_data.csv', encoding='utf-8')
    sub_df = df.head(30000)  # You may want to adjust the sample size temporarily for testing.
    
    # Preprocess the data (do this only once)
    X = preprocess_clustering_data(sub_df)

    # Evaluate clustering performance and determine the optimal k
    optimal_k = evaluate_clustering(X, max_k=8)
    logging.info(f"The optimal number of clusters is: {optimal_k}")

    # Train the clustering model with the optimal k
    logging.info(f"Training KMeans with optimal k={optimal_k}...")
    kmeans_model = train_clustering_model(sub_df, n_clusters=optimal_k)
    
    # Export predictions to a CSV file
    export_predictions(sub_df, kmeans_model)
