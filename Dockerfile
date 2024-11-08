# Dockerfile
FROM apache/airflow:2.5.1

USER airflow  

# Instalar kafka-python como usuario airflow
RUN pip install --no-cache-dir kafka-python

# Usar una imagen de Python
FROM python:3.9

# Establecer el directorio de trabajo en el contenedor
WORKDIR /app

# Copiar archivos de la aplicación
COPY kafka_consumer.py .

# Instalar librerías necesarias
RUN pip install kafka-python

# Comando predeterminado para ejecutar el consumidor
CMD ["python", "kafka_consumer.py"]
