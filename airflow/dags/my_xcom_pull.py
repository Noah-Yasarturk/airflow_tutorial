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


with DAG (
    'my_xcom_pull',
    default_args = my_args,
    description = 'Here I will pull the string I pushed',
    schedule_interval = timedelta(days=1),
    start_date = days_ago(1),
    tags = ['custom']
) as dag:


    # Pull the pushed xcom from another dag  
    def get_xcom(**kwargs):
        my_dag_id = 'my_xcom_push'
        my_task_id = 'printable_task'
        my_key = 'pushed_str'
        task_instance = kwargs['task_instance']
        pulled_xcomm_value = task_instance.xcom_pull(task_ids=my_task_id,
        dag_id=my_dag_id,key='pushed_str')
        print('I pulled the key ' + my_key + ' from task ' + my_task_id
        + ' of dag ' + my_dag_id)
        print('It has a value of ' + pulled_xcomm_value)
        # According to https://www.mikulskibartosz.name/use-xcom-pull-to-get-variable-from-another-dag/
        # xcom_pull / push is nigh on useless between DAGs. 
        # What's more, this is the wrong way to use the tool
        return pulled_xcomm_value 

    python_pull_task = PythonOperator(
        task_id = 'python_xcom_pull',
        python_callable = get_xcom,
        provide_context = True
    )

    python_pull_task