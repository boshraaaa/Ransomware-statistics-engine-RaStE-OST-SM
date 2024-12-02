from influxdb_client import InfluxDBClient
import pandas as pd

# InfluxDB Configuration
INFLUXDB_HOST = 'http://localhost:8086'  # InfluxDB URL
AUTH_TOKEN = 'lideTUM3U3sZwpmzk2sDbRayZ7zHKfZzo4U42doc24UZBzPznA1fjU-CeCxEglGuMnvYigYKLNo5S9oyvbjQsA=='  # InfluxDB authentication token
DEFAULT_BUCKET = 'test'  # InfluxDB bucket to query from
DEFAULT_ORG = 'ransomeware'  # Your InfluxDB organization

# Initialize InfluxDB Client
influx_client = InfluxDBClient(url=INFLUXDB_HOST, token=AUTH_TOKEN, org=DEFAULT_ORG)

# Query InfluxDB to get the last 2 years of data, and list all available keys (tags and fields)
def query_available_keys_influxdb():
    query = '''
    from(bucket: "test")
      |> range(start: -2y)  // Query for the last 2 years of data
      |> filter(fn: (r) => r._measurement == "indicator_data")  // Filter by the measurement name
      |> keys()  // This returns the keys (tags and fields) of the measurement
    '''
    result = influx_client.query_api().query(query=query)

    # Convert the result into a list of dictionaries (records)
    records = []
    for table in result:
        for record in table.records:
            records.append(record.values)
    
    # If we have data, convert to DataFrame, else print no data
    if records:
        keys = pd.DataFrame(records)
        print("Available Keys (Tags and Fields):")
        print(keys)
    else:
        print("No keys found.")

# Query InfluxDB to get the last 2 years of data for countryName, if available
def query_last_2_years_influxdb():
    query = '''
    from(bucket: "test")
      |> range(start: -2y)  // Query for the last 2 years of data
      |> filter(fn: (r) => r._measurement == "indicator_data")  // Filter by the measurement name
    '''
    result = influx_client.query_api().query(query=query)
    
    # Convert the result into a list of dictionaries (records)
    records = []
    for table in result:
        for record in table.records:
            records.append(record.values)
    
    # If we have data, convert to DataFrame, else print no data
    if records:
        df = pd.DataFrame(records)
        print("Data from InfluxDB (Last 2 Years):")
        print(df.head(10))  # Display first 10 rows to inspect the structure
    else:
        print("No data found for the last 2 years.")

# Main function to execute both queries
if __name__ == "__main__":
    # First, check all available keys (tags and fields)
    query_available_keys_influxdb()

    # Then, query the data from the last 2 years and check countryName if present
    query_last_2_years_influxdb()
