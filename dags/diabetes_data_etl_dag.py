from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from datetime import datetime

# Función para leer y transformar los datos de la tabla diabetes_data
def read_and_transform_data():
    # Conectar a PostgreSQL a través de PostgresHook
    postgres_hook = PostgresHook(postgres_conn_id='postgres_diabetes_data')
    conn = postgres_hook.get_conn()
    
    # Leer datos desde la tabla diabetes_data
    query = "SELECT * FROM diabetes_data;"
    diabetes_data = pd.read_sql(query, conn)

    # Transformaciones de datos aquí (ejemplo: limpiar datos, crear nuevas columnas, etc.)
    # Ejemplo de transformación: suponer que necesitamos agrupar o calcular promedios
    # Para el ejemplo, supongamos que solo seleccionamos algunas columnas y las agrupamos
    fact_table = diabetes_data[['patientid', 'bmi', 'systolicbp', 'diastolicbp', 'fastingbloodsugar', 
                                 'hba1c', 'cholesteroltotal', 'cholesterolldl', 'cholesterolhdl', 
                                 'cholesteroltriglycerides', 'qualityoflifescore', 'diagnosis']]

    dim_demography = diabetes_data[['patientid', 'age', 'gender', 'ethnicity', 'socioeconomicstatus', 
                                     'educationlevel']]
    
    dim_health_habits = diabetes_data[['patientid', 'smoking', 'alcoholconsumption', 'physicalactivity', 
                                         'dietquality', 'sleepquality']]

    # Guardar las tablas transformadas en archivos CSV temporales
    fact_table.to_csv('/opt/airflow/dags/files/fact_table.csv', index=False)
    dim_demography.to_csv('/opt/airflow/dags/files/dim_demography.csv', index=False)
    dim_health_habits.to_csv('/opt/airflow/dags/files/dim_health_habits.csv', index=False)

# Función para cargar los datos en PostgreSQL
def load_data_to_postgres():
    # Establecer conexión con PostgreSQL a través de PostgresHook
    postgres_hook = PostgresHook(postgres_conn_id='postgres_diabetes_data')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # Cargar la tabla de hechos (fact_table)
    fact_table_path = '/opt/airflow/dags/files/fact_table.csv'
    fact_table = pd.read_csv(fact_table_path)
    for _, row in fact_table.iterrows():
        cursor.execute("""
            INSERT INTO fact_table (patientid, bmi, systolicbp, diastolicbp, fastingbloodsugar, hba1c, 
                                    cholesteroltotal, cholesterolldl, cholesterolhdl, cholesteroltriglycerides, 
                                    qualityoflifescore, diagnosis)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, tuple(row))

    # Cargar la tabla de dimensiones (dim_demography)
    dim_demography_path = '/opt/airflow/dags/files/dim_demography.csv'
    dim_demography = pd.read_csv(dim_demography_path)
    for _, row in dim_demography.iterrows():
        cursor.execute("""
            INSERT INTO dim_demography (patientid, age, gender, ethnicity, socioeconomicstatus, educationlevel)
            VALUES (%s, %s, %s, %s, %s, %s)
            """, tuple(row))

    # Cargar la tabla de dimensiones (dim_health_habits)
    dim_health_habits_path = '/opt/airflow/dags/files/dim_health_habits.csv'
    dim_health_habits = pd.read_csv(dim_health_habits_path)
    for _, row in dim_health_habits.iterrows():
        cursor.execute("""
            INSERT INTO dim_health_habits (patientid, smoking, alcoholconsumption, physicalactivity, 
                                           dietquality, sleepquality)
            VALUES (%s, %s, %s, %s, %s, %s)
            """, tuple(row))

    # Confirmar la transacción y cerrar la conexión
    conn.commit()
    cursor.close()

# Configurar el DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 30),
    'retries': 1
}

# Crear el DAG y las tareas
with DAG('diabetes_data_etl_dag',
         schedule_interval='@daily',
         default_args=default_args,
         catchup=False) as dag:

    # Tarea 1: Leer y transformar los datos desde la tabla diabetes_data
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=read_and_transform_data
    )

    # Tarea 2: Cargar los datos transformados en PostgreSQL
    load_task = PythonOperator(
        task_id='load_data_to_postgres',
        python_callable=load_data_to_postgres
    )

    # Definir dependencias
    transform_task >> load_task
