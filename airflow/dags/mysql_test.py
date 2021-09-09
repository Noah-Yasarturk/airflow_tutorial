from airflow import DAG
from datetime import timedelta
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago 

my_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['Noah.Yasarturk@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Dag definition
with DAG(
    dag_id = 'mysql_test',
    default_args = my_args,
    description = 'Here I will test connection to MySQL and hopefully branching',
    schedule_interval = timedelta(days=1),
    start_date = days_ago(1),
    tags = ['custom']
) as dag:

    def get_conn_id():
        path = '/opt/airflow/sens.txt'
        sens_contents = ''
        with open(path, 'r') as f:
            sens_contents = f.read()
            print('Contents of sens: ' + sens_contents)
        


    printable_task = PythonOperator(
        task_id = 'print_conn_id',
        python_callable=get_conn_id
    )

    # Connect
    # mysql_task = MySqlOperator(
    #     task_id = 'query_task',
    #     mysql_conn_id = get_conn_id()
    # )

    printable_task