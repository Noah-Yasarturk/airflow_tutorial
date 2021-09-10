from airflow import DAG
from airflow.utils.dates import days_ago 
from airflow.operators.python_operator import PythonOperator

import datetime

my_args = {
    'owner': 'airflow',
    'depends_on_past': False
}

with DAG(
    dag_id = 'sample_jinja',
    default_args = my_args,
    schedule_interval='@daily',
    start_date = days_ago(1),
    tags = ['custom']
) as dag: 

    # From https://airflow.apache.org/docs/apache-airflow/stable/concepts/operators.html
    class DynamicPath:
        ''' 
        '''

        template_fields = ['path']

        def __init__(self, my_path):
            self.path = my_path


    def transform_data(*args):
        print('An object has been instanitated and provided.')
        created_path = args[0].path
        print('Here\'s its path: ' + created_path)
    
    task1 = PythonOperator(
        task_id = 'give_dynamic_path',
        python_callable = transform_data,
        op_args = [
            DynamicPath('/tmp/{{ ds }}/my_file')
        ],
        dag=dag
    )

    task1