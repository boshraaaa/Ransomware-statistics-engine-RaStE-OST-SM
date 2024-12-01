# Use a slim Python image as the base
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Copy the local flink_consumer directory to the container
COPY ./flink_consumer /app

# Install necessary dependencies
RUN pip install --no-cache-dir \
    pyflink \
    pandas \
    scikit-learn \
    influxdb-client \
    kafka-python \
    numpy

# Set the default command to run the Flink job
CMD ["python3", "flink_job.py"]
