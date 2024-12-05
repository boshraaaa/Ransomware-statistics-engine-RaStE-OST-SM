# Use Python 3.11 slim base image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Copy the contents of the Dashboard folder into the container
COPY . /app

# Install system dependencies for Python and InfluxDB client
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libffi-dev \
    libssl-dev && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose the port Dash will use
EXPOSE 8050

# Command to run the dashboard application
CMD ["python", "app.py"]
