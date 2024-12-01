# File: time_series_analysis.py
from influxdb import InfluxDBClient
from statsmodels.tsa.arima.model import ARIMA
import pandas as pd

# InfluxDB Client Configuration
client = InfluxDBClient(host='influxdb', port=8086, database='ransomware')

def fetch_data():
    # Query InfluxDB for processed data
    query = "SELECT * FROM processed_data WHERE time > now() - 7d"
    result = client.query(query)
    return pd.DataFrame(list(result.get_points()))

def predict_daily_activity(data):
    predictions = {}
    grouped = data.groupby('location_country')

    for country, group in grouped:
        series = group['indicator_id'].astype(float)
        if len(series) > 2:  # ARIMA requires at least 3 data points
            model = ARIMA(series, order=(1, 1, 1))
            fit = model.fit()
            predictions[country] = fit.forecast(steps=1)[0]

    return predictions

def write_predictions(predictions):
    points = [
        {
            "measurement": "predictions",
            "tags": {"country": country},
            "fields": {"daily_prediction": float(value)}
        }
        for country, value in predictions.items()
    ]
    client.write_points(points)

if __name__ == "__main__":
    data = fetch_data()
    predictions = predict_daily_activity(data)
    write_predictions(predictions)
