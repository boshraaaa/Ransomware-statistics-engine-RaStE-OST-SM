import json

def process_stream(stream):
    parsed_stream = stream.map(lambda x: json.loads(x))

    top_countries_stream = parsed_stream \
        .filter(lambda record: "location_country" in record) \
        .map(lambda record: (record["location_country"], 1)) \
        .key_by(lambda x: x[0]) \
        .reduce(lambda a, b: (a[0], a[1] + b[1])) \
        .map(lambda x: {"measurement": "top_countries", "tags": {"country": x[0]}, "fields": {"count": x[1]}})

    return top_countries_stream

