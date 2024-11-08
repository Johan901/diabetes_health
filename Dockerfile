# Dockerfile
FROM apache/airflow:2.5.1

USER airflow  

RUN pip install --no-cache-dir kafka-python

FROM python:3.9

WORKDIR /app

COPY kafka_consumer.py .

RUN pip install kafka-python

CMD ["python", "kafka_consumer.py"]
