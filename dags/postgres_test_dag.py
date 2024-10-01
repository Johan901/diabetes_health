from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime

# obtenemos datos de PostgreSQL
def fetch_data_from_postgres():
    pg_hook = PostgresHook(postgres_conn_id='postgres_diabetes_data')
    records = pg_hook.get_records("SELECT * FROM diabetes_data LIMIT 10;")  # Consulta de la tabla diabetes_data
    for row in records:
        print(row)  # Los registros se imprimirán en los logs de Airflow

# definimos el DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 27),
}

with DAG('test_postgres_connection_dag',
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:

    fetch_data_task = PythonOperator(
        task_id='fetch_data_from_postgres',
        python_callable=fetch_data_from_postgres
    )

    fetch_data_task  # No es necesario especificar dependencias si hay solo una tarea
