import streamlit as st
from confluent_kafka import Consumer, KafkaException
import json
import time
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Configuración del consumidor de Kafka
consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my_group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_config)
consumer.subscribe(['merged_data_topic'])

# Configuración del dashboard de Streamlit
st.set_page_config(layout="wide")
st.title("Dashboard de Salud en Tiempo Real")
st.subheader("Monitorización en tiempo real de métricas de salud")

# Variables de datos en tiempo real
deaths = []
timestamps = []

# Función para calcular métricas adicionales
def calcular_metricas(df):
    mean_deaths = df['deaths'].mean() if 'deaths' in df else 0
    count_entries = len(df)
    return mean_deaths, count_entries

# Contenedores para las métricas
col1, col2, col3 = st.columns(3)

# Contenedores para gráficos
st.write("**Visualización de Datos**")
col4, col5 = st.columns(2)
col6, col7 = st.columns(2)

# DataFrame para almacenar los datos en tiempo real
data = []

try:
    st.write('Esperando mensajes...')
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaException._PARTITION_EOF:
                st.write(f"Error: {msg.error()}")
            continue

        # Procesar los datos en tiempo real
        data_point = json.loads(msg.value().decode('utf-8'))
        data.append(data_point)
        df = pd.DataFrame(data)

        # Calcular métricas
        mean_deaths, count_entries = calcular_metricas(df)

        # Mostrar métricas en el dashboard
        col1.metric("Promedio de Muertes", f"{mean_deaths:.2f}")
        col2.metric("Total de Entradas", f"{count_entries}")

        # Primer gráfico - Distribución de Muertes (escala reducida)
        with col4:
            st.write("Distribución de Muertes")
            if 'deaths' in df:
                fig, ax = plt.subplots(figsize=(4, 2))  # Reducción de escala
                df['deaths'].value_counts().plot(kind='bar', ax=ax)
                ax.set_xlabel("Número de Muertes")
                ax.set_ylabel("Frecuencia")
                st.pyplot(fig)
                plt.close(fig)  # Cerramos la figura después de mostrarla

        # Segundo gráfico - Serie Temporal de Muertes (escala reducida)
        with col5:
            st.write("Serie Temporal de Muertes")
            if 'timestamp' in df and 'deaths' in df:
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
                fig, ax = plt.subplots(figsize=(4, 2))  # Reducción de escala
                df.set_index('timestamp')['deaths'].plot(ax=ax)
                ax.set_ylabel("Número de Muertes")
                st.pyplot(fig)
                plt.close(fig)  # Cerramos la figura después de mostrarla

        # Gráfico de barras de tendencias de muertes
        with col6:
            st.write("Tendencias de Muertes")
            fig, ax = plt.subplots(figsize=(4, 2))  # Reducción de escala
            ax.bar(df.index[:20], df['deaths'][:20], color='blue', alpha=0.7)
            ax.set_xlabel("Índice de Registro")
            ax.set_ylabel("Muertes")
            st.pyplot(fig)
            plt.close(fig)  # Cerramos la figura después de mostrarla

        # Gráfico de área para visualización acumulada de muertes
        with col7:
            st.write("Visualización Acumulada de Muertes")
            fig, ax = plt.subplots(figsize=(4, 2))  # Reducción de escala
            ax.fill_between(df.index[:20], df['deaths'][:20], color='red', alpha=0.3)
            ax.plot(df.index[:20], df['deaths'][:20], color='blue', alpha=0.7)
            ax.set_xlabel("Índice de Registro")
            ax.set_ylabel("Muertes")
            st.pyplot(fig)
            plt.close(fig)  # Cerramos la figura después de mostrarla

        # Nuevo gráfico de líneas - Tendencia de muertes
        st.write("Tendencia de Muertes en Tiempo Real")
        if 'timestamp' in df and 'deaths' in df:
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
            fig, ax = plt.subplots(figsize=(8, 3))  # Gráfico más ancho para la tendencia
            ax.plot(df['timestamp'], df['deaths'], color='green', linestyle='-', marker='o', markersize=3)
            ax.set_title("Tendencia de Muertes")
            ax.set_xlabel("Tiempo")
            ax.set_ylabel("Número de Muertes")
            st.pyplot(fig)
            plt.close(fig)

        # Matrices de correlación - solo las dos primeras (si hay suficientes datos)
        if len(df.columns) > 1:
            st.write("Matrices de Correlación (Primeras 2)")
            fig, ax = plt.subplots(1, 2, figsize=(10, 3))
            corr_matrix = df.corr()
            ax[0].imshow(corr_matrix, cmap='coolwarm', aspect='auto')
            ax[1].imshow(corr_matrix, cmap='coolwarm', aspect='auto')
            ax[0].set_title("Matriz de Correlación 1")
            ax[1].set_title("Matriz de Correlación 2")
            st.pyplot(fig)
            plt.close(fig)  # Cerramos la figura después de mostrarla

        # Tabla de datos (solo las primeras 5 entradas)
        st.write("**Tabla de Datos (Primeras 5 Entradas)**")
        st.dataframe(df.head(5))

        time.sleep(1)  # Intervalo de actualización

except KeyboardInterrupt:
    st.write("Consumo detenido.")
finally:
    consumer.close()
