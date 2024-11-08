from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

# Función para exportar datos de PostgreSQL a CSV
def export_to_csv():
    # Establecer conexión a PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id='postgres_diabetes_data')
    conn = postgres_hook.get_conn()
    
    # Definir el cursor
    cursor = conn.cursor()
    
    # Consultas para obtener los datos de las tablas
    queries = {
        'dim_patient': "SELECT * FROM dim_patient;",
        'dim_health_indicators': "SELECT * FROM dim_health_indicators;",
        'fact_diabetes': "SELECT * FROM fact_diabetes;",
        'fact_cdc': "SELECT * FROM fact_cdc;"
    }
    
    # Exportar datos a CSV
    for table_name, query in queries.items():
        # Ejecutar la consulta
        cursor.execute(query)
        
        # Obtener los resultados
        rows = cursor.fetchall()
        
        # Obtener nombres de las columnas
        colnames = [desc[0] for desc in cursor.description]
        
        # Crear DataFrame
        df = pd.DataFrame(rows, columns=colnames)
        
        # Ruta destino para guardar el CSV en el contenedor de Airflow
        file_path = f'/opt/airflow/dags/project_ETL/{table_name}.csv'
        
        # Exportar el DataFrame a CSV
        df.to_csv(file_path, index=False)
        
        print(f"{table_name} exportado correctamente como CSV a: {file_path}")
    
    # Cerrar la conexión
    cursor.close()
    conn.close()


# Definición del DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 11, 7),
    'retries': 1,
}

with DAG(
    'export_diabetes_data_to_csv',
    default_args=default_args,
    description='Exportar datos de PostgreSQL a CSV',
    schedule_interval=None,  # Cambiar si deseas que se ejecute en un horario específico
    catchup=False,
) as dag:
    
    # Tarea que ejecutará la función export_to_csv
    export_task = PythonOperator(
        task_id='export_data_to_csv',
        python_callable=export_to_csv
    )
