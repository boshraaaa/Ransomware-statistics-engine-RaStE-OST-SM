from influxdb_client import InfluxDBClient
import pandas as pd

# InfluxDB Configuration
INFLUXDB_HOST = 'http://localhost:8086'  # InfluxDB URL
AUTH_TOKEN = 'JkLVh_Glxl0FfIHnJM3C8HZOVvY_kG_spqDAJ4yK2HlhH7ia6oQqLf5IOy2XpvzMVlThyoFVjiAfsztM_CE8vw==' 
DEFAULT_BUCKET = 'prediction'  # InfluxDB bucket to query from
DEFAULT_ORG = 'ransomeware'  # Your InfluxDB organization

# Initialize InfluxDB Client
influx_client = InfluxDBClient(url=INFLUXDB_HOST, token=AUTH_TOKEN, org=DEFAULT_ORG)

# Query InfluxDB to get the last 2 years of data, and list all available keys (tags and fields)
def query_available_keys_influxdb():
    query = '''
    from(bucket: "ransomeware")
      |> range(start: -2y)  // Query for the last 2 years of data
      |> filter(fn: (r) => r._measurement == "top_10_target_countries")  // Filter by the measurement name
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
    from(bucket: "ransomware")
        |> range(start: -5y)  // Adjust the time range as needed
        |> filter(fn: (r) => r._measurement == "top_10_target_countries")
        |> group(columns: ["target_country"])
        |> sum(column: "_value")  // Sum the attack counts for each target_country
        |> sort(columns: ["_value"], desc: true)
        |> limit(n: 10)

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
        print(df.columns) 
        df.to_csv('top target countries data.csv')# Display first 10 rows to inspect the structure
    else:
        print("No data found for the last 2 years.")

# Main function to execute both queries
if __name__ == "__main__":
    # Then, query the data from the last 2 years and check countryName if present
    query_last_2_years_influxdb()
