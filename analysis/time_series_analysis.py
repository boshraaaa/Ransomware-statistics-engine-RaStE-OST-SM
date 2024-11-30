import pandas as pd
from influxdb_client import InfluxDBClient
from sktime.forecasting.compose import make_reduction
from sktime.forecasting.model_selection import temporal_train_test_split
from sktime.datasets import load_airline

# Fetch data from InfluxDB
client = InfluxDBClient(url="http://influxdb:8086", token="your_token")
query = 'from(bucket: "ransomware_db") |> range(start: -30d)'
result = client.query_api().query(query)
data = pd.DataFrame(result)

# Use sktime for time-series forecasting (example: using airline dataset)
y = load_airline()
y_train, y_test = temporal_train_test_split(y)

forecaster = make_reduction(RandomForestRegressor(), strategy="direct")
forecaster.fit(y_train)
y_pred = forecaster.predict(fh=[1, 2, 3])
print(y_pred)
