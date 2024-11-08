import streamlit as st
from kafka import KafkaConsumer
import json
import matplotlib.pyplot as plt

# Configuración de Kafka
consumer = KafkaConsumer(
    'merged_data_topic',  # Nombre del topic
    bootstrap_servers=['localhost:9093'],  # Kafka server
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Configura la interfaz de Streamlit
st.title('Real-Time Dashboard - Kafka Consumer')
st.subheader('Gráfico de Muertes en Tiempo Real')

deaths = []

# Esto mostrará los mensajes en tiempo real
try:
    st.write('Esperando mensajes...')
    for message in consumer:
        data = message.value
        deaths.append(data.get('deaths', 0))  # Agregar las muertes al listado
        
        # Mostrar los datos recibidos
        st.json(data)  # Mostrar los datos en formato JSON
        
        # Actualizar el gráfico de muertes en tiempo real
        st.write("Gráfico de muertes:")
        st.line_chart(deaths)  # Mostrar la evolución de las muertes

except KeyboardInterrupt:
    # Detener el consumidor de Kafka si interrumpimos el proceso (Ctrl + C)
    st.write("Consumo detenido.")
    consumer.close()
