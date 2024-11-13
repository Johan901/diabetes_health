from confluent_kafka import Producer
import json
import time

# Configuración del productor de Kafka
producer_config = {
    'bootstrap.servers': 'localhost:9092'  # puerto de Kafka
}

producer = Producer(producer_config)
topic = 'merged_data_topic'  # Nombre del tópico al envia mensajes

# Función de callback para manejar errores de entrega
def delivery_report(err, msg):
    if err is not None:
        print(f"Error al entregar el mensaje: {err}")
    else:
        print(f"Mensaje enviado a {msg.topic()} [{msg.partition()}]")

# Enviar mensajes de prueba al tópico
for i in range(10):  # Enviar 10 mensajes como ejemplo
    data = {'deaths': i, 'timestamp': time.time()}  # Datos de ejemplo
    producer.produce(topic, json.dumps(data).encode('utf-8'), callback=delivery_report)
    producer.poll(0)  # Llamar para manejar los mensajes pendientes
    time.sleep(1)  # Enviar un mensaje por segundo para simular datos en tiempo real

# Asegurarse de que todos los mensajes han sido enviados
producer.flush()
