from influxdb_client import InfluxDBClient, Point
import logging

class InfluxDBWriter:
    def __init__(self,INFLUXDB_HOST, AUTH_TOKEN, DEFAULT_ORGANIZATION):
        """
        Initialize the InfluxDBWriter with connection details.
        """
        INFLUXDB_HOST = 'http://localhost:8086'
        AUTH_TOKEN = 'JkLVh_Glxl0FfIHnJM3C8HZOVvY_kG_spqDAJ4yK2HlhH7ia6oQqLf5IOy2XpvzMVlThyoFVjiAfsztM_CE8vw==' 
        DEFAULT_BUCKET = 'ransomware'
        PREDICTION_BUCKET = 'prediction'
        DEFAULT_ORGANIZATION = 'ransomeware'
        # InfluxDB Configuration
        self.INFLUXDB_HOST = INFLUXDB_HOST
        self.AUTH_TOKEN = AUTH_TOKEN
        self.DEFAULT_ORGANIZATION = DEFAULT_ORGANIZATION
        self.client = InfluxDBClient(url=INFLUXDB_HOST, token=AUTH_TOKEN, org=DEFAULT_ORGANIZATION)
        self.write_api = self.client.write_api()
        self.delete_api = self.client.delete_api()
    

    def saveToInfluxDB(self, df, bucket):
        """
        Save DataFrame records to InfluxDB.
        Args:
            df (pd.DataFrame): DataFrame containing the data to write.
            bucket (str): The InfluxDB bucket where data will be written.
            timestamp_column (str): Column in the DataFrame used as a timestamp.
        """
        try:
            # Convert DataFrame to InfluxDB format
            self.write_api.write(
                bucket=bucket,
                org=self.DEFAULT_ORGANIZATION,
                record=df,
                data_frame_timestamp_column="created_indicator",
                data_frame_measurement_name="Ransomware",  # Default measurement name
                #data_frame_timestamp_column=timestamp_column
            )
            logging.info(f"Successfully wrote {len(df)} data points to InfluxDB bucket '{bucket}'.")
        except Exception as e:
            logging.error(f"Error writing data to InfluxDB: {e}")

    def flushInfluxDB(self, bucket, start="1970-01-01T00:00:00Z", stop="now()"):
        """
        Flush existing data in the specified bucket.
        Args:
            bucket (str): The InfluxDB bucket to flush.
            start (str): Start time for deletion (default: epoch start).
            stop (str): Stop time for deletion (default: now).
        """
        try:
            self.delete_api.delete(
                start,
                stop,
                '_measurement="attack"',
                bucket=bucket,
                org=self.org
            )
            logging.info(f"Flushed data in bucket '{bucket}' from {start} to {stop}.")
        except Exception as e:
            logging.error(f"Error flushing data from InfluxDB: {e}")
