# Use the official Apache Spark image as the base
FROM python:3.11-slim

# Set the working directory
WORKDIR /app

# Install Python and pip
RUN apt-get update && apt-get install -y python3 python3-pip

# Copy dependency file and install requirements
COPY requirements.txt /app/
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy application script
COPY spark_consumer.py /app/

# Set default command to run the Spark application
CMD ["python3", "spark_consumer.py"]
