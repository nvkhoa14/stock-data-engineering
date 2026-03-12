from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import sys

# Import functions from the ELT scripts
sys.path.append('/home/nvkhoa14/stock-data-engineering/backend-db/ETL/extract')
sys.path.append('/home/nvkhoa14/stock-data-engineering/backend-db/ETL/transform')
sys.path.append('/home/nvkhoa14/stock-data-engineering/backend-db/ETL/load')
from crawl_companies import crawl_companies
from crawl_markets import crawl_markets
from transform_to_db_1 import transform_to_database_1
from transform_to_db_2 import transform_to_database_2
from transform_to_db_3 import transform_to_database_3
from load_db_1 import load_json_to_db_1
from load_db_2 import load_json_to_db_2
from load_db_3 import load_json_to_db_3

# Dags arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Dafinition of the DAG
with DAG(
    dag_id='ETL_to_Database',
    default_args=default_args,
    description='A simple ETL DAG',
    schedule_interval='1 0 * * *',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Task 1: Crawl companies
    crawl_companies_task = PythonOperator(
        task_id='crawl_companies',
        python_callable=crawl_companies,
    )

    # Task 2: Crawl markets
    crawl_markets_task = PythonOperator(
        task_id='crawl_markets',
        python_callable=crawl_markets,
    )

    # Task 3: Transform to database 1
    transform_to_database_1_task = PythonOperator(
        task_id='transform_to_database_1',
        python_callable=transform_to_database_1,
    )

    # Task 4: Load JSON to DB 1
    load_json_to_db_1_task = PythonOperator(
        task_id='load_json_to_db_1',
        python_callable=load_json_to_db_1,
    )

    # Task 5: Transform to database 2
    transform_to_database_2_task = PythonOperator(
        task_id='transform_to_database_2',
        python_callable=transform_to_database_2,
    )

    # Task 6: Load JSON to DB 2
    load_json_to_db_2_task = PythonOperator(
        task_id='load_json_to_db_2',
        python_callable=load_json_to_db_2,
    )

    # Task 7: Transform to database 3
    transform_to_database_3_task = PythonOperator(
        task_id='transform_to_database_3',
        python_callable=transform_to_database_3,
    )

    # Task 8: Load JSON to DB 3
    load_json_to_db_3_task = PythonOperator(
        task_id='load_json_to_db_3',
        python_callable=load_json_to_db_3,
    )

    crawl_companies_task >> transform_to_database_1_task >> load_json_to_db_1_task >> transform_to_database_2_task >> load_json_to_db_2_task >> transform_to_database_3_task >> load_json_to_db_3_task
    crawl_markets_task >> transform_to_database_1_task