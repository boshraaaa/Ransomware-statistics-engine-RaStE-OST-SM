import os
import pandas as pd
import logging
import pickle
import matplotlib.pyplot as plt
from pmdarima import auto_arima
from statsmodels.tsa.statespace.sarimax import SARIMAX
from sklearn.preprocessing import LabelEncoder

# Logging Configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Paths for Saving Models and Predictions
BASE_PATH = "C:/Users/I745988/Ransomware-attack/spark/Models/Prediction/"
os.makedirs(BASE_PATH, exist_ok=True)

# Data Preprocessing Function
def preprocess_data(df):
    """Prepare data for time-series forecasting."""
    logging.info("Preprocessing data...")

    # Drop unused columns
    columns_to_drop = [
        'content', 'title', 'description_indicator', 'expiration', 
        'is_active', 'in_group', 'is_subscribing', 
        'description_pulse', 'author_name', 'modified', 'public', 
        'adversary'
    ]
    df.drop(columns=columns_to_drop, inplace=True, errors='ignore')

    # Handle datetime features
    if 'created_indicator' not in df.columns:
        raise ValueError("'created_indicator' column is required for time-series processing.")
    
    df['created_indicator'] = pd.to_datetime(df['created_indicator'], errors='coerce')
    df['daily_attack_count'] = df.groupby(['source_country', df['created_indicator'].dt.date])['id_indicator'].transform('count')

    # Encode categorical variables
    label_encoders = {}
    for column in ['source_country', 'target_country', 'malware_family']:
        if column in df.columns:
            le = LabelEncoder()
            df[column + '_encoded'] = le.fit_transform(df[column].astype(str))
            label_encoders[column] = le
    
    logging.info("Data preprocessing completed.")
    return df, label_encoders

# Train ARIMA Model using pmdarima
def train_arima_model(time_series):
    """Train an ARIMA model for a given time series."""
    logging.info("Training ARIMA model...")
    model = auto_arima(time_series, seasonal=False, stepwise=True, suppress_warnings=True, error_action="ignore")
    return model

# Train SARIMAX Model using statsmodels
def train_sarimax_model(time_series):
    """Train a SARIMAX model for a given time series."""
    logging.info("Training SARIMAX model...")
    model = SARIMAX(time_series, order=(1, 1, 1), seasonal_order=(1, 1, 1, 7))
    fitted_model = model.fit(disp=False)
    return fitted_model

# Forecast Future Values
def forecast_future(model, periods):
    """Forecast future values based on the fitted model."""
    logging.info(f"Forecasting next {periods} steps...")
    y_pred = model.predict(n_periods=periods) if hasattr(model, "predict") else model.forecast(steps=periods)
    return y_pred

# Plot Time-Series Predictions
def plot_future_predictions(time_series, y_pred, country, model_type):
    """Plot historical and future predictions."""
    plt.figure(figsize=(12, 6))
    plt.plot(time_series, label="Historical Data", color='blue')
    future_index = pd.date_range(start=time_series.index[-1], periods=len(y_pred) + 1, freq='D')[1:]
    plt.plot(future_index, y_pred, label="Future Predictions", color='red')
    plt.title(f"{model_type.upper()} Model for {country} - Future Forecast")
    plt.xlabel('Time')
    plt.ylabel('Daily Attack Counts')
    plt.legend()
    plt.grid()
    plt.tight_layout()
    plt.savefig(os.path.join(BASE_PATH, f"{country}_{model_type}_future_forecast.png"))
    plt.show()

# Additional Analytics per Country
def malware_family_distribution(df, country):
    """Compute the distribution of malware families for a given country."""
    country_df = df[df['source_country'] == country]
    distribution = country_df['malware_family'].value_counts(normalize=True)
    logging.info(f"Malware family distribution for {country}:\n{distribution}")
    return distribution

# Main Program
if __name__ == "__main__":
    try:
        logging.info("Loading data...")
        # Load data
        df = pd.read_csv("C:/Users/I745988/Ransomware-attack/data/enriched_data.csv")
        df = df.sample(frac=1, random_state=42).reset_index(drop=True)

        # Preprocess data
        df, label_encoders = preprocess_data(df)

        # Identify Top 10 Countries with Most Attacks
        top_countries = (
            df.groupby('source_country')['daily_attack_count'].sum()
            .nlargest(10)
            .index.tolist()
        )

        logging.info(f"Top 10 countries with most attacks: {top_countries}")

        results = []

        for country in top_countries:
            logging.info(f"Processing country: {country}")

            # Prepare time series for the country
            country_data = df[df['source_country'] == country]
            time_series = (
                country_data.groupby('created_indicator')['daily_attack_count']
                .sum()
                .sort_index()
            )
            time_series.index = pd.to_datetime(time_series.index)  # Ensure datetime index

            # Train models
            arima_model = train_arima_model(time_series)
            sarimax_model = train_sarimax_model(time_series)

            # Forecast future values (e.g., next 30 days)
            future_periods = 30
            arima_pred = forecast_future(arima_model, future_periods)
            sarimax_pred = forecast_future(sarimax_model, future_periods)

            # Plot ARIMA future predictions
            plot_future_predictions(time_series, arima_pred, country, model_type='arima')

            # Plot SARIMAX future predictions
            plot_future_predictions(time_series, sarimax_pred, country, model_type='sarimax')

            # Malware family distribution
            distribution = malware_family_distribution(df, country)

            # Save models
            with open(os.path.join(BASE_PATH, f"{country}_arima_model.pkl"), 'wb') as f:
                pickle.dump(arima_model, f)
            sarimax_model.save(os.path.join(BASE_PATH, f"{country}_sarimax_model.pkl"))

            # Append results
            results.append({
                'country': country,
                'malware_family_distribution': distribution
            })

        # Save aggregated results
        results_df = pd.DataFrame(results)
        results_df.to_csv(os.path.join(BASE_PATH, "top_countries_analytics_summary.csv"), index=False)
        logging.info("Top countries analytics summary saved.")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
