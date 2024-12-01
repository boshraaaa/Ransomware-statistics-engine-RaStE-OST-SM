import logging
from influxdb_client import Point

def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    return logging.getLogger(__name__)



def prepare_influxdb_data(row):
    try:
        point = Point("ransomware_activity") \
            .tag("country", row.get('location_country', 'Unknown')) \
            .field("indicator_id", row['indicator_id']) \
            .field("pulse_name", row['pulse_name']) \
            .field("pulse_revision", row['pulse_revision']) \
            .field("latitude", row.get('location_latitude', 0.0)) \
            .field("longitude", row.get('location_longitude', 0.0))
        return point
    except Exception as e:
        print(f"Error preparing data for InfluxDB: {e}")
        return None

