from datetime import timedelta
from airflow import DAG
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
    dag_id='my_xcom_push',
    default_args = my_args,
    description = 'Here I will push a string to pull elsewere',
    schedule_interval = timedelta(days=1),
    start_date = days_ago(1),
    tags = ['custom']
) as dag:

    # Python callables
    def give_list(end_range):
        """Creates and returns a list of ints"""
        l = [5+i for i in range(0,end_range)]
        print('List in give_list: {s}'.format(s=l))
        return l

    def give_string(**kwargs):
        ''' Simply prints a string that is also pushed to xcom'''
        s = "Noah Yasarturk"
        print('Pushing str ' + s + ' to Xcom')
        # 'ti' seems somehow necessary, although why is not referenced
        # see https://github.com/apache/airflow/blob/main/airflow/example_dags/example_xcom.py
        kwargs['ti'].xcom_push(key="pushed_str", value=s)

    # Task 1 - return a list to pull elsewhere 
    runnable_task = PythonOperator(
        task_id='return_list_task',
        python_callable=give_list,
        op_kwargs = {"end_range": 10}
    )


    printable_task = PythonOperator(
        task_id = 'print_string_task',
        python_callable=give_string
    )

    runnable_task.doc_md = """This task creates a list in python"""
    
    printable_task >> runnable_task
