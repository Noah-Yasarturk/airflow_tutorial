from datetime import timedelta
from textwrap import dedent
from airflow import DAG
from airflow.utils.dates import days_ago 
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.mysql_operator import MySqlOperator as BaseMySqlOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import BranchPythonOperator


my_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['Noah.Yasarturk@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
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

    class ReturningMySqlOperator(BaseMySqlOperator):
        '''
        The normal MySqlOperator does not return the data that it queries. This one does. 
        From https://stackoverflow.com/questions/52645905/how-to-use-mysqloperator-with-xcom-in-airflow
        '''
        def execute(self, context):
            self.log.info('Executing: %s', self.sql) # log the query being run
            hook = MySqlHook(mysql_conn_id=self.mysql_conn_id,
                            schema=self.database)
            return hook.get_first(
                self.sql,
                parameters=self.parameters)

    def print_records(**kwargs):
        '''Pulls the results from the query_task, prints, and returns'''
        ti = kwargs['ti']
        xcom = ti.xcom_pull(task_ids='query_task')
        string_out = 'Value pulled from MySQL: {}'.format(xcom)
        print(string_out)
        return string_out


    def branch_logic(ti):
        ''' Pulls results from our SQL query, routing where appropriate
        '''
        xcom_value = int(ti.xcom_pull(task_ids='query_task')[0])
        print('Pulled this value from query_task: {}'.format(str(xcom_value)))
        if xcom_value >= 5:
            return 'proceed_bash'
        else:
            return 'sleep_bash'

    # Connect & query
    mysql_task = ReturningMySqlOperator(
        task_id = 'query_task',
        mysql_conn_id = 'dev_playground',
        sql = 'SELECT COUNT(*) c FROM test',
        # do_xcom_push=True # MySqlOperator does not support do_xcom_push
    )

    # Results will be in xcom. Pull them & print them 
    printable_task = PythonOperator(
        task_id = 'print_records',
        python_callable=print_records
    )

    branch_task = BranchPythonOperator(
        task_id = 'branch_on_record_size',
        python_callable = branch_logic
    )

    templated_command = dedent(
    """
    {% for i in range(5) %}
        echo "{{ ds }} Good to go!!"
    {% endfor %}
    """
    )

    templated_command_2 = dedent(
        """
        echo "Not enough data! Going to sleep ..."
        sleep 5
        """
    )

    proceed_task = BashOperator(
        task_id ='proceed_bash',
        bash_command=templated_command
    )

    stop_task = BashOperator(
        task_id = 'sleep_bash',
        bash_command= templated_command_2
    )

    printable_task.doc_md = ''' 
    '''

    mysql_task >> printable_task >> branch_task >> [proceed_task,stop_task]


    