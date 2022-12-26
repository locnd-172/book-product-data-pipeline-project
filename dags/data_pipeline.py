from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import scripts.crawl_product_id

default_args = {
    'owner': 'Loc Nguyen',
    'start_date': days_ago(0),
    'email': ['lc.nguyendang123@gmail.com'],
    'email_on_failure': False,
    'email_on-retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def greet(ti):
    first_name = ti.xcom_pull(task_ids='get_name', key='first_name')
    last_name = ti.xcom_pull(task_ids='get_name', key='last_name')
    age = ti.xcom_pull(task_ids='get_age', key='age')
    print(f"Hello, My name is {first_name} {last_name}! I'm {age} years old")
    
def get_name(ti):
    ti.xcom_push(key='first_name', value='Loc')
    ti.xcom_push(key='last_name', value='Nguyen')

def get_age(ti):
    ti.xcom_push(key='age', value=20)
    
with DAG(
    dag_id='automate_data_pipeline',
    default_args=default_args,
    description='Data pipeline to process Tiki\'s books data',
    start_date=datetime(2022, 12, 24),
    schedule_interval='@daily'
) as dag:
    
    start_operator = DummyOperator(task_id='start_pipeline')
    
    task1 = PostgresOperator(
        task_id='create_table_product_id',
        postgres_conn_id='postgresql',
        sql='''
            CREATE TABLE IF NOT EXISTS staging.book_product_id (
                product_id CHARACTER VARYING NOT NULL PRIMARY KEY
            )
        ''',
    )
   
    task2 = PythonOperator(
        task_id='crawl_product_id',
        python_callable=scripts.crawl_product_id.main
    )
    
    end_operator = DummyOperator(task_id='stop_pipeline')

    start_operator >> task1 >> task2 >> end_operator