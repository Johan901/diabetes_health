from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from datetime import datetime

# leemos y transformamos los datos de la tabla original
def read_and_transform_data():
    # Conectamos a PostgreSQL a través de PostgresHook
    postgres_hook = PostgresHook(postgres_conn_id='postgres_diabetes_data')
    conn = postgres_hook.get_conn()
    
    
    query = "SELECT * FROM diabetes_data;"
    diabetes_data = pd.read_sql(query, conn)

    # Transformacion de los datos 
    # obtenemos columnas y las agrupamos
    fact_table = diabetes_data[['patientid', 'bmi', 'systolicbp', 'diastolicbp', 'fastingbloodsugar', 
                                 'hba1c', 'cholesteroltotal', 'cholesterolldl', 'cholesterolhdl', 
                                 'cholesteroltriglycerides', 'qualityoflifescore', 'diagnosis']]

    dim_demography = diabetes_data[['patientid', 'age', 'gender', 'ethnicity', 'socioeconomicstatus', 
                                     'educationlevel']]
    
    dim_health_habits = diabetes_data[['patientid', 'smoking', 'alcoholconsumption', 'physicalactivity', 
                                         'dietquality', 'sleepquality']]

    # Guardamos las tablas transformadas en archivos CSV temporales
    fact_table.to_csv('/opt/airflow/dags/files/fact_table.csv', index=False)
    dim_demography.to_csv('/opt/airflow/dags/files/dim_demography.csv', index=False)
    dim_health_habits.to_csv('/opt/airflow/dags/files/dim_health_habits.csv', index=False)

# cargamos los datos en PostgreSQL
def load_data_to_postgres():
    # Establecer conexión con PostgreSQL a través de PostgresHook
    postgres_hook = PostgresHook(postgres_conn_id='postgres_diabetes_data')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # tabla de hechos (fact_table)
    fact_table_path = '/opt/airflow/dags/files/fact_table.csv'
    fact_table = pd.read_csv(fact_table_path)
    for _, row in fact_table.iterrows():
        cursor.execute("""
            INSERT INTO fact_table (patientid, bmi, systolicbp, diastolicbp, fastingbloodsugar, hba1c, 
                                    cholesteroltotal, cholesterolldl, cholesterolhdl, cholesteroltriglycerides, 
                                    qualityoflifescore, diagnosis)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, tuple(row))

    # tabla de dimensiones (dim_demography)
    dim_demography_path = '/opt/airflow/dags/files/dim_demography.csv'
    dim_demography = pd.read_csv(dim_demography_path)
    for _, row in dim_demography.iterrows():
        cursor.execute("""
            INSERT INTO dim_demography (patientid, age, gender, ethnicity, socioeconomicstatus, educationlevel)
            VALUES (%s, %s, %s, %s, %s, %s)
            """, tuple(row))

    # tabla de dimensiones (dim_health_habits)
    dim_health_habits_path = '/opt/airflow/dags/files/dim_health_habits.csv'
    dim_health_habits = pd.read_csv(dim_health_habits_path)
    for _, row in dim_health_habits.iterrows():
        cursor.execute("""
            INSERT INTO dim_health_habits (patientid, smoking, alcoholconsumption, physicalactivity, 
                                           dietquality, sleepquality)
            VALUES (%s, %s, %s, %s, %s, %s)
            """, tuple(row))

    
    conn.commit()
    cursor.close()

# DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 30),
    'retries': 1
}

#  DAG y sus tareas
with DAG('diabetes_data_etl_dag',
         schedule_interval='@daily',
         default_args=default_args,
         catchup=False) as dag:

    # Tarea 1 transformar los datos 
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=read_and_transform_data
    )

    # Tarea 2 cargar los datos transformados en PostgreSQL
    load_task = PythonOperator(
        task_id='load_data_to_postgres',
        python_callable=load_data_to_postgres
    )

    transform_task >> load_task
