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
    'retry_delay': timedelta(minutes=2)
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

    # def get_conn_id():
    #     path = '/opt/airflow/sens.txt'
    #     sens_contents = ''
    #     with open(path, 'r') as f:
    #         sens_contents = f.read()
    #         print('Contents of sens: ' + sens_contents)
    #     # Create dict from results
    #     d = {}
    #     for l in sens_contents.split('\n'):
    #         s = l.split(':')
    #         d[s[0]] = s[1]
    #     # Using dict, create connection string

    def get_records(**kwargs):
        '''Pulls the results from the query_task, prints, and returns'''
        ti = kwargs['ti']
        xcom = ti.xcom_pull(task_ids='query_task')
        string_out = 'Value pulled from MySQL: {}'.format(xcom)
        print(string_out)
        return string_out

    # Connect & query
    mysql_task = MySqlOperator(
        task_id = 'query_task',
        mysql_conn_id = 'dev_playground',
        sql = 'SELECT * FROM test'
    )

    # Results will be in xcom. Pull them
    printable_task = PythonOperator(
        task_id = 'print_records',
        python_callable=get_records
    )

    printable_task.doc_md = ''' In this case, 
    printable_task printed no records.
    Airflow is an orchestration tool, not a data manipulation
    tool. The work should be done by the files it executes.
    It is not like Apache NiFi in this regard.

    Data is not to be passed between tasks. Tasks are atomic. 
    How then should we do conditional logic?? Next branch.
    '''

    mysql_task >> printable_task


    