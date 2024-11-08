from kafka import KafkaConsumer
import json

def consume_messages():
    # Crear el consumidor conectado al mismo servidor de Kafka 
    consumer = KafkaConsumer(
        'merged_data_topic',  # topic
        bootstrap_servers=['localhost:9093'],  # host de Kafka en el entorno Docker
        auto_offset_reset='earliest',  
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print("Consuming messages from 'merged_data_topic'...")
    for message in consumer:
        print(f"Message received: {message.value}")

    consumer.close()

if __name__ == "__main__":
    consume_messages()
