# Use the official Apache Spark image as the base
FROM bitnami/spark:latest

COPY requirements.txt .


# Install Python dependencies for Spark
RUN pip install --no-cache-dir -r requirements.txt

# Copy your Spark consumer application script into the container
COPY spark_consumer.py .

# Set the default command to run the Spark application
CMD ["python", "spark_consumer.py"]
