from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import scripts.sql_statements
import scripts.extract_product_id
import scripts.extract_product_data
import scripts.extract_product_review

default_args = {
    'owner': 'Loc Nguyen',
    'start_date': days_ago(0),
    'email': ['lc.nguyendang123@gmail.com'],
    'email_on_failure': False,
    'email_on-retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}
    
with DAG(
    dag_id='automate_data_pipeline',
    default_args=default_args,
    description='Data pipeline to process Tiki\'s books data',
    start_date=datetime(2022, 12, 24),
    schedule_interval='@daily',
    tags=['data-pipeline', 'etl', 'book-product']
) as dag:
    
    start_operator = DummyOperator(task_id='start_pipeline')
    
    create_staging_book_product_id_table = PostgresOperator(
        task_id='create_staging_book_product_id_table',
        postgres_conn_id='postgresql',
        sql=scripts.sql_statements.create_staging_book_product_id_table,
    )
    
    create_staging_book_product_data_table = PostgresOperator(
        task_id='create_staging_book_product_data_table',
        postgres_conn_id='postgresql',
        sql=scripts.sql_statements.create_staging_book_product_data_table,
    )
    
    create_staging_book_product_review_table = PostgresOperator(
        task_id='create_staging_book_product_review_table',
        postgres_conn_id='postgresql',
        sql=scripts.sql_statements.create_staging_book_product_review_table,
    )
   
    extract_product_id = PythonOperator(
        task_id='extract_product_id',
        python_callable=scripts.extract_product_id.main
    )
    
    extract_product_data = PythonOperator(
        task_id='extract_product_data',
        python_callable=scripts.extract_product_data.main
    )
    
    extract_product_review = PythonOperator(
        task_id='extract_product_review',
        python_callable=scripts.extract_product_review.main
    )
    
    end_operator = DummyOperator(task_id='stop_pipeline')

    start_operator >> [create_staging_book_product_id_table, create_staging_book_product_data_table, create_staging_book_product_review_table] >> extract_product_id
    extract_product_id >> [extract_product_data, extract_product_review] >> end_operator