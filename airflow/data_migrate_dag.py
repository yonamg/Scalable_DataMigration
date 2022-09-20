from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.hooks.mysql_hook import MySqlHook
from pendulum import datetime
from datetime import timedelta
from airflow import DAG 

####################################################
#     Data Migration from PostgreSQL to MySQL      #
####################################################

def migrate():
    '''
    A function to migrate data from postgres to MySQL database
    '''
    src = PostgresHook(postgres_conn_id="pg_server")
    target= MySqlHook(mysql_conn_id="mysql_conn")
    src_conn = src.get_conn()
    cursor = src_conn.cursor() 

    cursor.execute("SELECT * FROM traffic")
    sources = cursor.fetchall()  
    target.insert_rows(table="traffic",rows=sources,replace=True,replace_index="id")

####################################################
#          Airflow DAG configurations              #
####################################################

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    "start_date": datetime(2022, 7, 20, 2, 30, 00),
    'retry_delay': timedelta(minutes=5),
}    

with DAG(
    "Migrate_data_from_PostgreSQL_to_MySQL", 
    description="A data migration DAG from Postgres to MySQL database.",
    schedule_interval=None,
    catchup=False,
    default_args=default_args, 
) as dag:   
 
      create_table = MySqlOperator(
        task_id="create_table",
        mysql_conn_id="mysql_conn",
        sql='./trafficDB_schema.sql' 
    )
      migrate_data = PythonOperator(task_id="migrate_data",
        python_callable=migrate)


####################################################
#          Task dependencies                       #
####################################################

create_table >> migrate_data