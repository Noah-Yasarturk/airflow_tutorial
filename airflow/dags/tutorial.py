from datetime import timedelta
from textwrap import dedent
# Needed to instantiate DAG
from airflow import DAG

# Needed to Operate
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago 
# Args to be passed into each operator 
# Can be overriden on a per-task basis
default_args = {
    'owner':'airflow',
    'depends_on_past': False,
    'email':['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='tutorial', 
    default_args = default_args,
    description = 'A simple tutorial DAG',
    schedule_interval = timedelta(days=1),
    start_date = days_ago(2),
    tags = ['example']
) as dag:

    # Tasks below created by instantiating operators
    t1 = BashOperator(
        task_id='print_date',
        bash_command='date'
    )

    t2 = BashOperator(
        task_id = 'sleep',
        depends_on_past=False,
        bash_command='sleep 5',
        retries=3
    )

    t1.doc_md = dedent(
        """\
    #### Task Documentation
     The doc_md attribute can be used to document text:
     doc_md is markdown, doc is plain text, doc_rst, doc_json,
     doc_yaml all get rendered into UI's Task Instance Details page.
     ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
    """
    )

    dag.doc_md = __doc__ # provides docstring at the beginning of a DAG
    dag.doc_md = """
    This is a documentation placed anywhere.\n
    I have now edited it.
    """ # otherwise,type as such

    
    templated_command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
        echo "{{ params.my_param }}"
        {% endfor %}
        """
    )

    t3 = BashOperator(
        task_id='templated',
        depends_on_past=False,
        bash_command=templated_command,
        params={'my_param': 'Parameter I passed in'},
    )

    t1 >> [t2,t3]
