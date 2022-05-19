from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.postgres_operator import PostgresOperator
from postgres_conn import get_data



from fetchDataAndStoreInCSV import *

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 5, 19),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(seconds=20),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG("WeatherAnalysis", default_args=default_args, schedule_interval=timedelta(1),template_searchpath=['/usr/local/airflow/weather_sql'])

#t1=BashOperator(task_id='check_file_exists', bash_command='shasum ~/store_files_airflow/raw_store_transactions.csv', retries=2, retry_delay=timedelta(seconds=15), dag = dag)

t1 = PythonOperator(task_id='FetchAndUploadWeatherData', python_callable=execute_required, dag = dag, retries = 1)

t2 = PostgresOperator(task_id='create_table', postgres_conn_id="postgres_conn", sql="createtable.sql", dag=dag)

t3 = PythonOperator(task_id='Load_Data_Into_Table', python_callable=get_data, dag=dag, retries = 1)


t1 >> t2 >> t3



