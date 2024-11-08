import streamlit as st
from kafka import KafkaConsumer
import json
import matplotlib.pyplot as plt

# CKafka
consumer = KafkaConsumer(
    'merged_data_topic',  # topic
    bootstrap_servers=['localhost:9093'],  # kafka server
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

st.title('Real-Time Dashboard - Kafka Consumer')
st.subheader('Gráfico de Muertes en Tiempo Real')

deaths = []

try:
    st.write('Esperando mensajes...')
    for message in consumer:
        data = message.value
        deaths.append(data.get('deaths', 0))  
        
        st.json(data)  
        
        st.write("Gráfico de muertes:")
        st.line_chart(deaths)  

except KeyboardInterrupt:
    st.write("Consumo detenido.")
    consumer.close()
