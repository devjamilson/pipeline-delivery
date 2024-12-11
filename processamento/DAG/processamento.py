from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def extract_files():
    return 


with DAG(
    dag_id='ETL_delivery',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2024, 12, 11),
        'email_on_failure': False,
        'email_on_retry': False,
    },
    description='DAG para processar os dados de delivery',
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    extract = PythonOperator(
        task_id=f'extract_files',
        python_callable=extract_files
    )
    
    extract