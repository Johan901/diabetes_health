from confluent_kafka import Consumer, KafkaException
import json

def consume_messages():
    # Configuración del consumidor de Kafka
    consumer_config = {
        'bootstrap.servers': 'localhost:9093',  # Servidor de Kafka
        'group.id': 'my_group',                 # Grupo de consumidores
        'auto.offset.reset': 'earliest'         # Inicia desde el principio del topic
    }
    
    consumer = Consumer(consumer_config)
    consumer.subscribe(['merged_data_topic'])  # Suscribirse al topic

    print("Consumiendo mensajes desde 'merged_data_topic'...")

    try:
        while True:
            msg = consumer.poll(1.0)  # Espera por un mensaje

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    print('End of partition reached')
                else:
                    print(f"Error: {msg.error()}")
                continue

            data = json.loads(msg.value().decode('utf-8'))
            print(f"Mensaje recibido: {data}")
    except KeyboardInterrupt:
        print("Consumo detenido.")
    finally:
        consumer.close()

if __name__ == "__main__":
    consume_messages()
