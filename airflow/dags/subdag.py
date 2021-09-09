from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

# Ripped directly from the docs: https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html#concepts-subdags

def subdag(parent_dag_name, child_dag_name, args):
    '''
    Generate a DAG to be used as a subdag

    :param str parent_dag_name: Id of the parent DAG
    :param str child_dag_name: Id of the child DAG
    :param dict args: Default arguments to provide to the subdag
    :return: DAG to use as a subdag
    :rtype: airflow.models.DAG
    '''
    dag_subdag = DAG(
        dag_id=f"{parent_dag_name}.{child_dag_name}",
        default_args=args,
        start_date=days_ago(2),
        schedule_interval="@daily"
    )

    for i in range(5):
        DummyOperator(
            task_id=f"{child_dag_name}-task-{i + 1}",
            default_args=args,
            dag=dag_subdag
        )
        
    return dag_subdag