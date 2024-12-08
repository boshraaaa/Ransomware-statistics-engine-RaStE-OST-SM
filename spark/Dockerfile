# Use the official Python image as the base
FROM python:3.11-slim

# Set the working directory
WORKDIR /app

RUN apt-get update && apt-get install -y \
    openjdk-17-jre-headless \
    procps \
    curl \
    && apt-get clean


# Set environment variables for Java and Spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"
ENV KAFKA_BROKER=kafka:9092
ENV INFLUXDB_HOST=http://influxdb:8086
ENV INFLUXDB_ORG=ransomware
ENV INFLUXDB_BUCKET=ransomware
ENV AUTH_TOKEN=JkLVh_Glxl0FfIHnJM3C8HZOVvY_kG_spqDAJ4yK2HlhH7ia6oQqLf5IOy2XpvzMVlThyoFVjiAfsztM_CE8vw==

# Copy the requirements file
COPY requirements.txt /app/

# Install Python dependencies
RUN pip install --no-cache-dir pyspark==3.4.0
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application script
COPY spark_consumer.py /app/

COPY Models /app/Models

# Set default command to run the Spark application
CMD ["python3", "spark_consumer.py"]
