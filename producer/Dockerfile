FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

RUN apt-get update && apt-get install -y netcat-openbsd && apt-get clean


COPY producer_API.py /app/

CMD ["python", "producer_API.py"]
