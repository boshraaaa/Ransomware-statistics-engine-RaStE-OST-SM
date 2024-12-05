from pyflink.datastream.connectors.influxdb import InfluxDBSink

def create_influxdb_sink():
    influxdb_sink = InfluxDBSink.builder() \
        .set_url("http://influxdb:8086") \
        .set_database("ransomware_bucket") \
        .set_org("my-org") \
        .set_token("admin-token") \
        .build()

    return influxdb_sink
