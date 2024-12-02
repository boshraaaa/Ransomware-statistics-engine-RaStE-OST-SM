import pandas as pd
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, accuracy_score
from sklearn.preprocessing import LabelEncoder
import pickle
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def preprocess_prediction_data(df):
    """Preprocess data for prediction (feature extraction and scaling)"""
    logging.info("Preprocessing data for prediction...")

    # Drop unnecessary columns
    columns_to_drop = [
        'content', 'title', 'description_indicator', 'expiration',
        'is_active', 'in_group', 'is_subscribing',
        'description_pulse', 'author_name', 'modified', 'public',
        'adversary'
    ]
    df = df.drop(columns=columns_to_drop, errors='ignore')  # Avoid key errors

    # Feature Engineering: Convert 'created_indicator' to timestamp
    df['created_indicator_timestamp'] = pd.to_datetime(
        df['created_indicator'], errors='coerce'
    ).astype('int64', errors='ignore') / 10**9

    # Select relevant features
    feature_columns = ['source_latitude', 'source_longitude', 'target_latitude', 'target_longitude', 'created_indicator_timestamp']
    X = df[feature_columns].fillna(0)  # Handle missing values
    return X

def train_attack_number_prediction(df):
    """Train a regression model to predict the number of attacks per source country"""
    logging.info("Training attack number prediction model...")

    # Aggregate attacks by source country for numerical target
    attack_counts = df.groupby('source_country').size().reset_index(name='attack_count')
    df = df.merge(attack_counts, on='source_country', how='left')
    y = df['attack_count']

    X = preprocess_prediction_data(df)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    rf_regressor = RandomForestRegressor(n_estimators=100, random_state=42)
    rf_regressor.fit(X_train, y_train)

    # Predictions
    y_pred = rf_regressor.predict(X_test)
    mse = mean_squared_error(y_test, y_pred)

    logging.info(f"Mean Squared Error for Attack Number Prediction: {mse:.4f}")
    print(f'Mean Squared Error for Attack Number Prediction: {mse:.4f}')

    # Export predictions
    prediction_df = pd.DataFrame({
        'Actual': y_test,
        'Predicted': y_pred
    })
    prediction_df.to_csv('C:/Users/I745988/Ransomware-attack/spark/Models/Predictionattack_number_predictions.csv', index=False)

    # Save the model
    with open('C:/Users/I745988/Ransomware-attack/spark/Models/Predictionrf_attack_number_source_country.pkl', 'wb') as f:
        pickle.dump(rf_regressor, f)

    return rf_regressor

def train_malware_family_prediction(df):
    """Train a classification model to predict malware family"""
    logging.info("Training malware family prediction model...")

    # Encode malware_family if it contains strings
    if df['malware_family'].dtype == 'object':
        label_encoder = LabelEncoder()
        df['malware_family'] = label_encoder.fit_transform(df['malware_family'].astype(str))

    y = df['malware_family']

    X = preprocess_prediction_data(df)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    rf_classifier = RandomForestClassifier(n_estimators=100, random_state=42)
    rf_classifier.fit(X_train, y_train)

    # Predictions
    y_pred = rf_classifier.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)

    logging.info(f"Accuracy for Malware Family Prediction: {accuracy:.4f}")
    print(f'Accuracy for Malware Family Prediction: {accuracy:.4f}')

    # Export predictions
    prediction_df = pd.DataFrame({
        'Actual': y_test,
        'Predicted': y_pred
    })
    prediction_df.to_csv('C:/Users/I745988/Ransomware-attack/spark/Models/Predictionmalware_family_predictions.csv', index=False)

    # Save the model
    with open('C:/Users/I745988/Ransomware-attack/spark/Models/Prediction/rf_malware_family.pkl', 'wb') as f:
        pickle.dump(rf_classifier, f)

    return rf_classifier

if __name__ == "__main__":
    # Load data
    logging.info("Loading data...")
    df = pd.read_csv('C:/Users/I745988/Ransomware-attack/data/enriched_data.csv', encoding='utf-8')
    sub_df = df.head(3000)
    # Train models for predictions
    logging.info("Training models for predictions...")
    train_attack_number_prediction(sub_df)
    train_malware_family_prediction(sub_df)
    logging.info("Prediction model training complete.")
