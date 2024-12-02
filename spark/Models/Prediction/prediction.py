import pandas as pd
import matplotlib.pyplot as plt
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, accuracy_score, classification_report
from sklearn.preprocessing import LabelEncoder
import pickle
import logging
import os
import pandas as pd
from sklearn.preprocessing import LabelEncoder, StandardScaler
from datetime import datetime
import logging
# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Paths for models and predictions
PREDICTION_PATH = "C:/Users/I745988/Ransomware-attack/spark/Models/Prediction/"
os.makedirs(PREDICTION_PATH, exist_ok=True)



def ip_to_int(ip):
    """Convert IP address to integer representation."""
    import socket
    import struct
    try:
        return struct.unpack("!I", socket.inet_aton(ip))[0]
    except socket.error:
        return None

def preprocess_data(df):
    """Preprocess data by handling missing values, encoding, and feature engineering."""
    logging.info("Starting data preprocessing...")

    # Drop unnecessary columns
    columns_to_drop = [
        'content', 'title', 'description_indicator', 'expiration', 
        'is_active', 'in_group', 'is_subscribing', 
        'description_pulse', 'author_name', 'modified', 'public', 
        'adversary'
    ]
    df.drop(columns=columns_to_drop, inplace=True, errors='ignore')

    # Handle datetime conversion
    if 'created_indicator' not in df.columns:
        logging.error("'created_indicator' column is missing!")
        return None, None
    df['created_indicator'] = pd.to_datetime(df['created_indicator'], errors='coerce')

    if df['created_indicator'].isna().any():
        logging.warning("Some 'created_indicator' values could not be converted to datetime.")

    # Extract features from datetime
    df['year'] = df['created_indicator'].dt.year
    df['month'] = df['created_indicator'].dt.month
    df['day'] = df['created_indicator'].dt.day
    df['day_of_week'] = df['created_indicator'].dt.dayofweek
    df['hour'] = df['created_indicator'].dt.hour

    # Convert created_indicator to timestamp
    df['created_indicator_timestamp'] = df['created_indicator'].astype('int64') / 10**9

    # Cumulative attack calculation
    if 'source_country' in df.columns:
        df['cumulative_attacks'] = df.groupby(['source_country'])['created_indicator'].rank().fillna(0)
    else:
        logging.warning("'source_country' column is missing for cumulative attack calculation.")

    # Normalize latitude and longitude
    scaler = StandardScaler()
    latitude_longitude_columns = [
        ('source_latitude', 'source_longitude'),
        ('target_latitude', 'target_longitude')
    ]
    for lat_col, lon_col in latitude_longitude_columns:
        if lat_col in df.columns and lon_col in df.columns:
            df[[lat_col, lon_col]] = scaler.fit_transform(df[[lat_col, lon_col]])
        else:
            logging.warning(f"Columns {lat_col} and/or {lon_col} are missing for normalization.")

    # Encode IP addresses to integer representation
    if 'ip' in df.columns:
        df['ip_encoded'] = df['ip'].apply(ip_to_int)
    else:
        logging.warning("'ip' column is missing for IP encoding.")

    # Encode categorical columns
    label_encoders = {}
    categorical_columns = ['source_city', 'source_country', 'target_country', 'malware_family']
    for col in categorical_columns:
        if col in df.columns:
            le = LabelEncoder()
            df[col + '_encoded'] = le.fit_transform(df[col].astype(str))
            label_encoders[col] = le
        else:
            logging.warning(f"'{col}' column is missing for encoding.")

    logging.info("Data preprocessing complete.")
    return df, label_encoders


def train_attack_count_model(df):
    """Train a regression model to predict daily attack counts."""
    logging.info("Training attack count prediction model...")

    if 'source_country' not in df.columns or 'created_indicator' not in df.columns:
        logging.error("Missing required columns for attack count model!")
        return None, None

    # Aggregate daily attack counts
    daily_agg = df.groupby(['source_country', 'year', 'month', 'day']).size().reset_index(name='daily_attack_count')
    df = df.merge(daily_agg, on=['source_country', 'year', 'month', 'day'], how='left')

    # Ensure 'daily_attack_count' is in the dataframe
    if 'daily_attack_count' not in df.columns:
        logging.error("Column 'daily_attack_count' not found after aggregation!")
        return None, None

    X = df[['source_country_encoded', 'year', 'month', 'day', 'day_of_week', 'cumulative_attacks']].fillna(0)
    y = df['daily_attack_count']

    # Split data
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Train Random Forest Regressor
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    # Predictions and MSE calculation
    y_pred = model.predict(X_test)
    mse = mean_squared_error(y_test, y_pred)
    logging.info(f"Attack Count Prediction MSE: {mse:.4f}")

    # Save predictions
    prediction_df = pd.DataFrame({'Actual': y_test, 'Predicted': y_pred})
    prediction_df.to_csv(os.path.join(PREDICTION_PATH, "attack_count_predictions.csv"), index=False)

    # Save the model
    with open(os.path.join(PREDICTION_PATH, "rf_attack_count_model.pkl"), 'wb') as f:
        pickle.dump(model, f)

    return model, prediction_df, df  # Return updated dataframe

def train_malware_family_model(df):
    logging.info("Training malware family classification model...")

    # Ensure 'daily_attack_count' is present
    if 'daily_attack_count' not in df.columns:
        logging.error("Column 'daily_attack_count' not found!")
        return None, None

    # Check that 'malware_family_encoded' is present
    if 'malware_family_encoded' not in df.columns:
        logging.error("Column 'malware_family_encoded' is missing!")
        return None, None

    # Features and target
    X = df[['source_country_encoded', 'target_country_encoded', 'year', 'month', 'day', 'daily_attack_count']].fillna(0)
    y = df['malware_family_encoded']

    # Train-test split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Train Random Forest Classifier
    rf_classifier = RandomForestClassifier(n_estimators=100, random_state=42)
    rf_classifier.fit(X_train, y_train)

    # Evaluation
    y_pred = rf_classifier.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    logging.info(f"Malware Family Classification Accuracy: {accuracy:.4f}")

    # Save predictions
    prediction_df = pd.DataFrame({'Actual': y_test, 'Predicted': y_pred})
    prediction_df.to_csv(os.path.join(PREDICTION_PATH, "malware_family_predictions.csv"), index=False)

    # Save model
    with open(os.path.join(PREDICTION_PATH, "rf_malware_family_model.pkl"), 'wb') as f:
        pickle.dump(rf_classifier, f)

    return rf_classifier, prediction_df

def train_target_country_model(df):
    logging.info("Training target country prediction model...")

    # Ensure all required columns are present
    required_columns = ['daily_attack_count', 'malware_family_encoded', 'source_country_encoded', 'year', 'month', 'day']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        logging.error(f"Missing required columns: {missing_columns}")
        return None, None

    # Features and target
    X = df[['source_country_encoded', 'year', 'month', 'day', 'daily_attack_count', 'malware_family_encoded']].fillna(0)
    y = df['target_country_encoded']

    # Train-test split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Train Random Forest Classifier
    rf_classifier = RandomForestClassifier(n_estimators=100, random_state=42)
    rf_classifier.fit(X_train, y_train)

    # Evaluation
    y_pred = rf_classifier.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    logging.info(f"Target Country Prediction Accuracy: {accuracy:.4f}")

    # Save predictions
    prediction_df = pd.DataFrame({'Actual': y_test, 'Predicted': y_pred})
    prediction_df.to_csv(os.path.join(PREDICTION_PATH, "target_country_predictions.csv"), index=False)

    # Save model
    with open(os.path.join(PREDICTION_PATH, "rf_target_country_model.pkl"), 'wb') as f:
        pickle.dump(rf_classifier, f)

    return rf_classifier, prediction_df

def plot_predictions(prediction_df, model_name, df, time_column='created_indicator'):
    logging.info(f"Plotting predictions for {model_name}...")
    prediction_df = prediction_df.copy()
    prediction_df[time_column] = df.loc[prediction_df.index, time_column]
    prediction_df[time_column] = pd.to_datetime(prediction_df[time_column], errors='coerce')

    plt.figure(figsize=(10, 6))
    plt.plot(prediction_df[time_column], prediction_df['Actual'], label='Actual', color='blue')
    plt.plot(prediction_df[time_column], prediction_df['Predicted'], label='Predicted', color='red')
    plt.title(f"{model_name} - Actual vs Predicted Over Time")
    plt.xlabel('Time')
    plt.ylabel('Values')
    plt.legend()
    plt.grid()
    plt.tight_layout()
    plt.savefig(os.path.join(PREDICTION_PATH, f"{model_name}_predictions.png"))
    plt.show()

if __name__ == "__main__":
    logging.info("Starting the main program...")
    try:
        # Load and sample data
        df = pd.read_csv("C:/Users/I745988/Ransomware-attack/data/enriched_data.csv")
        df = df.sample(frac=1, random_state=42).reset_index(drop=True)
        df = df.head(30000)

        # Preprocess the data
        df, label_encoders = preprocess_data(df)
        if df is not None:
            # Train attack count model and update dataframe
            attack_count_model, attack_count_predictions, df = train_attack_count_model(df)
            if attack_count_model is not None:
                plot_predictions(attack_count_predictions, "Attack Count Model", df)

            # Train malware family classification model
            malware_family_model, malware_family_predictions = train_malware_family_model(df)
            if malware_family_model is not None:
                plot_predictions(malware_family_predictions, "Malware Family Model", df)

            # Train target country prediction model
            target_country_model, target_country_predictions = train_target_country_model(df)
            if target_country_model is not None:
                plot_predictions(target_country_predictions, "Target Country Model", df)

    except Exception as e:
        logging.error(f"An error occurred: {e}")
