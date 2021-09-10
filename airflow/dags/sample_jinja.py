from airflow import DAG
from airflow.utils.dates import days_ago 
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
import datetime

my_args = {
    'owner': 'airflow',
    'depends_on_past': False
}

def days_since(starting_date):
    return (datetime.datetime.now() - starting_date)

my_macros = {
    'birthday': datetime.datetime(1997, 4, 9), # Macro can be a variable
    'calc_my_age': days_since # Macro can also be a function
}

with DAG(
    dag_id = 'sample_jinja',
    default_args = my_args,
    schedule_interval='@daily',
    start_date = days_ago(1),
    tags = ['custom'],
    user_defined_macros = my_macros
) as dag: 

    # From https://airflow.apache.org/docs/apache-airflow/stable/concepts/operators.html
    class DynamicPath:
        ''' 
        '''
        # defines what fields CAN be templated
        # (operated on using Jinja)
        template_fields = ['path']

        def __init__(self, my_path):
            self.path = my_path


    def display_class_field(*args):
        print('An object has been instanitated and provided.')
        created_path = args[0].path
        print('Here\'s its path: ' + created_path)
    
    task1 = PythonOperator(
        task_id = 'give_dynamic_path',
        python_callable = display_class_field,
        op_args = [
            DynamicPath('/tmp/{{ ds }}/my_file')
        ],
        dag=dag
    )

    # From https://www.astronomer.io/guides/templating
    print_days = BashOperator(
        task_id = 'print_days',
        bash_command = "echo Day since my birthday of {{ birthday }} is {{ calc_my_age(birthday) }}"
    )

    task1 >> print_days