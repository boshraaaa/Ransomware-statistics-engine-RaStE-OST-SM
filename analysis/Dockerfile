FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install Python dependencies for Spark
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code into the container
COPY time_series_analysis.py .

CMD ["python3", "time_series_analysis.py"]  # Update this to the script you want to run
