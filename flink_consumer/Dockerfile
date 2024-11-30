FROM apache/flink:1.16.0-scala_2.12

# Install Python and dependencies
RUN apt-get update && apt-get install -y python3 python3-pip
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install -r /tmp/requirements.txt

# Copy all Python files into the container
WORKDIR /app
COPY . /app/

CMD ["python3", "flink_job.py"]
