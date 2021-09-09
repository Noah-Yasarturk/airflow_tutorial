from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago 
from airflow.operators.mysql_operator import MySqlOperator as BaseMySqlOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.python import PythonOperator


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

    # Connect & query
    mysql_task = ReturningMySqlOperator(
        task_id = 'query_task',
        mysql_conn_id = 'dev_playground',
        sql = 'SELECT COUNT(*) c FROM test',
        # do_xcom_push=True # MySqlOperator does not support do_xcom_push
    )

    # Results will be in xcom. Pull them
    printable_task = PythonOperator(
        task_id = 'print_records',
        python_callable=print_records
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


    