from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
from kafka import KafkaProducer
import json
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'cdc_diabetes_etl_dag',
    default_args=default_args,
    description='ETL pipeline for diabetes data and CDC API data',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 11, 5),
    catchup=False,
)

def extract_postgres():
    postgres_hook = PostgresHook(postgres_conn_id='postgres_diabetes_data')
    conn = postgres_hook.get_conn()
    query = "SELECT * FROM diabetes_data"
    df = pd.read_sql(query, conn)
    df.to_csv('/tmp/diabetes_data.csv', index=False)
    conn.close()

def extract_api():
    response = requests.get("https://data.cdc.gov/resource/bi63-dtpu.json")
    if response.status_code != 200:
        raise Exception(f"Error fetching data: {response.status_code}, {response.text}")
    data = response.json()
    df = pd.DataFrame(data)
    df.to_csv('/tmp/cdc_data.csv', index=False)

def transform_postgres():
    df = pd.read_csv('/tmp/diabetes_data.csv')
    # Aplicar transformaciones necesarias al dataset de diabetes aquí
    df['bmi'] = df['bmi'].fillna(df['bmi'].mean())  # Ejemplo de imputación de valores nulos
    df.to_csv('/tmp/transformed_diabetes_data.csv', index=False)

def transform_api():
    df = pd.read_csv('/tmp/cdc_data.csv')
    # Aplicar transformaciones necesarias al dataset de CDC aquí
    df['deaths'] = df['deaths'].fillna(0)  # Ejemplo de imputación de valores nulos
    df.to_csv('/tmp/transformed_cdc_data.csv', index=False)

def merge_data():
    df_diabetes = pd.read_csv('/tmp/transformed_diabetes_data.csv')
    df_cdc = pd.read_csv('/tmp/transformed_cdc_data.csv')
    # Unir ambos datasets. Como no tienen columnas comunes, usaremos `concat`.
    merged_df = pd.concat([df_diabetes, df_cdc], axis=0, ignore_index=True)
    merged_df.to_csv('/tmp/merged_data.csv', index=False)

def load_to_kafka():
    producer = KafkaProducer(
        bootstrap_servers=['kafka:9093'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    merged_df = pd.read_csv('/tmp/merged_data.csv')
    for _, row in merged_df.iterrows():
        producer.send('merged_data_topic', row.to_dict())

    producer.flush()
    producer.close()

def load_dimensional_model():
    postgres_hook = PostgresHook(postgres_conn_id='postgres_diabetes_data')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    
    #tablas de hechos y dimensiones
    cursor.execute("""CREATE TABLE IF NOT EXISTS dim_patient (
                        patientid SERIAL PRIMARY KEY,
                        age INT,
                        gender VARCHAR,
                        ethnicity VARCHAR,
                        socioeconomicstatus VARCHAR,
                        educationlevel VARCHAR
                    );""")
    
    cursor.execute("""CREATE TABLE IF NOT EXISTS dim_health_indicators (
                        indicator_id SERIAL PRIMARY KEY,
                        bmi FLOAT,
                        smoking BOOLEAN,
                        alcoholconsumption BOOLEAN,
                        physicalactivity VARCHAR,
                        dietquality VARCHAR,
                        sleepquality VARCHAR,
                        qualityoflifescore INT,
                        heavymetalsexposure BOOLEAN,
                        occupationalexposurechemicals BOOLEAN,
                        waterquality VARCHAR
                    );""")

    cursor.execute("""CREATE TABLE IF NOT EXISTS fact_diabetes (
                        fact_id SERIAL PRIMARY KEY,
                        patientid INT REFERENCES dim_patient(patientid),
                        diagnosis VARCHAR,
                        fastingbloodsugar FLOAT,
                        hba1c FLOAT,
                        systolicbp FLOAT,
                        diastolicbp FLOAT,
                        medicationadherence BOOLEAN,
                        healthliteracy BOOLEAN
                    );""")

    cursor.execute("""CREATE TABLE IF NOT EXISTS fact_cdc (
                        fact_id SERIAL PRIMARY KEY,
                        year INT,
                        cause_name VARCHAR,
                        state VARCHAR,
                        deaths INT,
                        aadr FLOAT
                    );""")

    conn.commit()
    
    # Insertar datos en las tablas de hechos y dimensiones desde el merge
    merged_df = pd.read_csv('/tmp/merged_data.csv')

    for _, row in merged_df.iterrows():
        # limpiar los datos antes de insertar
        age = int(row['age']) if pd.notnull(row['age']) else None
        gender = str(row['gender']) if pd.notnull(row['gender']) else None
        ethnicity = str(row['ethnicity']) if pd.notnull(row['ethnicity']) else None
        socioeconomicstatus = str(row['socioeconomicstatus']) if pd.notnull(row['socioeconomicstatus']) else None
        educationlevel = str(row['educationlevel']) if pd.notnull(row['educationlevel']) else None

        
        cursor.execute("""INSERT INTO dim_patient (age, gender, ethnicity, socioeconomicstatus, educationlevel)
                          VALUES (%s, %s, %s, %s, %s) 
                          ON CONFLICT (patientid) DO NOTHING;""",
                       (age, gender, ethnicity, socioeconomicstatus, educationlevel))

        bmi = float(row['bmi']) if pd.notnull(row['bmi']) else None
        smoking = bool(row['smoking']) if pd.notnull(row['smoking']) else None
        alcoholconsumption = bool(row['alcoholconsumption']) if pd.notnull(row['alcoholconsumption']) else None
        physicalactivity = str(row['physicalactivity']) if pd.notnull(row['physicalactivity']) else None
        dietquality = str(row['dietquality']) if pd.notnull(row['dietquality']) else None
        sleepquality = str(row['sleepquality']) if pd.notnull(row['sleepquality']) else None
        qualityoflifescore = int(row['qualityoflifescore']) if pd.notnull(row['qualityoflifescore']) else None
        heavymetalsexposure = bool(row['heavymetalsexposure']) if pd.notnull(row['heavymetalsexposure']) else None
        occupationalexposurechemicals = bool(row['occupationalexposurechemicals']) if pd.notnull(row['occupationalexposurechemicals']) else None
        waterquality = str(row['waterquality']) if pd.notnull(row['waterquality']) else None

        cursor.execute("""INSERT INTO dim_health_indicators (bmi, smoking, alcoholconsumption, physicalactivity, dietquality, 
                                                               sleepquality, qualityoflifescore, heavymetalsexposure, 
                                                               occupationalexposurechemicals, waterquality)
                          VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                          ON CONFLICT DO NOTHING;""",
                       (bmi, smoking, alcoholconsumption, physicalactivity, dietquality, sleepquality, qualityoflifescore, 
                        heavymetalsexposure, occupationalexposurechemicals, waterquality))

        diagnosis = str(row['diagnosis']) if pd.notnull(row['diagnosis']) else None
        fastingbloodsugar = float(row['fastingbloodsugar']) if pd.notnull(row['fastingbloodsugar']) else None
        hba1c = float(row['hba1c']) if pd.notnull(row['hba1c']) else None
        systolicbp = float(row['systolicbp']) if pd.notnull(row['systolicbp']) else None
        diastolicbp = float(row['diastolicbp']) if pd.notnull(row['diastolicbp']) else None
        medicationadherence = bool(row['medicationadherence']) if pd.notnull(row['medicationadherence']) else None
        healthliteracy = bool(row['healthliteracy']) if pd.notnull(row['healthliteracy']) else None

        cursor.execute("""INSERT INTO fact_diabetes (patientid, diagnosis, fastingbloodsugar, hba1c, systolicbp, diastolicbp, 
                                                       medicationadherence, healthliteracy)
                          VALUES (currval(pg_get_serial_sequence('dim_patient', 'patientid')), %s, %s, %s, %s, %s, %s, %s);""",
                       (diagnosis, fastingbloodsugar, hba1c, systolicbp, diastolicbp, medicationadherence, healthliteracy))
        
    conn.commit()
    cursor.close()
    conn.close()



# Definir tareas del DAG
extract_postgres_task = PythonOperator(task_id='extract_postgres', python_callable=extract_postgres, dag=dag)
extract_api_task = PythonOperator(task_id='extract_api', python_callable=extract_api, dag=dag)
transform_postgres_task = PythonOperator(task_id='transform_postgres', python_callable=transform_postgres, dag=dag)
transform_api_task = PythonOperator(task_id='transform_api', python_callable=transform_api, dag=dag)
merge_data_task = PythonOperator(task_id='merge_data', python_callable=merge_data, dag=dag)
load_to_kafka_task = PythonOperator(task_id='load_to_kafka', python_callable=load_to_kafka, dag=dag)
load_dimensional_model_task = PythonOperator(task_id='load_dimensional_model', python_callable=load_dimensional_model, dag=dag)

# Definir dependencias
extract_postgres_task >> transform_postgres_task
extract_api_task >> transform_api_task
[transform_postgres_task, transform_api_task] >> merge_data_task
merge_data_task >> [load_to_kafka_task, load_dimensional_model_task]
