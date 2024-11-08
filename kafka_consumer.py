from kafka import KafkaConsumer
import json

def consume_messages():
    # Crear el consumidor, conectado al mismo servidor de Kafka especificado en tu `docker-compose.yml`
    consumer = KafkaConsumer(
        'merged_data_topic',  # Nombre del topic
        bootstrap_servers=['localhost:9093'],  # Host de Kafka en el entorno Docker
        auto_offset_reset='earliest',  # Comienza desde el inicio del log
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print("Consuming messages from 'merged_data_topic'...")
    for message in consumer:
        # Procesar el mensaje (aquí simplemente se imprime, pero puedes realizar otras operaciones)
        print(f"Message received: {message.value}")

    consumer.close()

if __name__ == "__main__":
    consume_messages()
