from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

def export_to_csv():
    postgres_hook = PostgresHook(postgres_conn_id='postgres_diabetes_data')
    conn = postgres_hook.get_conn()
    
    cursor = conn.cursor()
    
    queries = {
        'dim_patient': "SELECT * FROM dim_patient;",
        'dim_health_indicators': "SELECT * FROM dim_health_indicators;",
        'fact_diabetes': "SELECT * FROM fact_diabetes;",
        'fact_cdc': "SELECT * FROM fact_cdc;"
    }
    
    # Exportar datos a CSV
    for table_name, query in queries.items():
        cursor.execute(query)
        
        rows = cursor.fetchall()
        
        colnames = [desc[0] for desc in cursor.description]
        
        df = pd.DataFrame(rows, columns=colnames)
        
        file_path = f'/opt/airflow/dags/project_ETL/{table_name}.csv'
        
        df.to_csv(file_path, index=False)
        
        print(f"{table_name} exportado correctamente como CSV a: {file_path}")
    
    cursor.close()
    conn.close()


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 11, 7),
    'retries': 1,
}

with DAG(
    'export_diabetes_data_to_csv',
    default_args=default_args,
    description='Exportar datos de PostgreSQL a CSV',
    schedule_interval=None,  
    catchup=False,
) as dag:
    
    
    export_task = PythonOperator(
        task_id='export_data_to_csv',
        python_callable=export_to_csv
    )
