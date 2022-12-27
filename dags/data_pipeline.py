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
import scripts.create_fact_book_product_table
import scripts.create_dim_book_table
import scripts.create_dim_category_table
import scripts.create_dim_review_table
import scripts.transform_load_dim_book
import scripts.transform_load_dim_category
import scripts.transform_load_dim_review
import scripts.transform_load_fact_book_product
import scripts.check_data_quality_report

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
    
    create_fact_book_product_table = PythonOperator(
        task_id='create_fact_book_product_table',
        python_callable=scripts.create_fact_book_product_table.main
    )
    
    create_dim_book_table = PythonOperator(
        task_id='create_dim_book_table',
        python_callable=scripts.create_dim_book_table.main
    )
    
    create_dim_category_table = PythonOperator(
        task_id='create_dim_category_table',
        python_callable=scripts.create_dim_category_table.main
    )
    
    create_dim_review_table = PythonOperator(
        task_id='create_dim_review_table',
        python_callable=scripts.create_dim_review_table.main
    )
    
    transform_load_dim_book = PythonOperator(
        task_id='transform_load_dim_book',
        python_callable=scripts.transform_load_dim_book.main
    )
    
    transform_load_dim_category = PythonOperator(
        task_id='transform_load_dim_category',
        python_callable=scripts.transform_load_dim_category.main
    )
    
    transform_load_dim_review = PythonOperator(
        task_id='transform_load_dim_review',
        python_callable=scripts.transform_load_dim_review.main
    )
    
    transform_load_fact_book_product = PythonOperator(
        task_id='transform_load_fact_book_product',
        python_callable=scripts.transform_load_fact_book_product.main
    )
    
    check_data_quality = PythonOperator(
        task_id='check_data_quality',
        python_callable=scripts.check_data_quality_report.main
    )
    
    end_operator = DummyOperator(task_id='stop_pipeline')

    start_operator >> [create_staging_book_product_id_table, create_staging_book_product_data_table, create_staging_book_product_review_table] >> extract_product_id
    extract_product_id >> [extract_product_data, extract_product_review] >> create_fact_book_product_table
    create_fact_book_product_table >> [create_dim_book_table, create_dim_category_table, create_dim_review_table]
    create_dim_book_table.set_downstream(transform_load_dim_book)
    create_dim_category_table.set_downstream(transform_load_dim_category)
    create_dim_review_table.set_downstream(transform_load_dim_review)
    [transform_load_dim_book, transform_load_dim_category, transform_load_dim_review] >> transform_load_fact_book_product >> check_data_quality >> end_operator