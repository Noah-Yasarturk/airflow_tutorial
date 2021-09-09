from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator

# Torn straight from https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html#concepts-taskgroups

with DAG(
    dag_id='sample_task_group',
    default_args={ "owner": 'airflow', },
    start_date = days_ago(1),
    schedule_interval="@once",
    tags=['example']
) as dag:

    with TaskGroup("group1") as group1:
        task1 = DummyOperator(task_id="task1")
        task2 = DummyOperator(task_id="task2")

    task3 = DummyOperator(task_id="task3")

    # task1 and task2 become upstream of task3 because both are grouped together here
    group1 >> task3

    '''
    Other than cleanliness/UI, how is this different than lists ??
    '''
