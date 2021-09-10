from airflow import DAG
from airflow.utils.dates import days_ago 
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import BranchPythonOperator
from textwrap import dedent


my_args = {
    'owner': 'airflow',
    'depend_on_past': False
}

with DAG(
    dag_id='sample_bash',
    description='Here I\'ll try to make a branching flow based on a nonzero bash exit code',
    default_args = my_args,
    schedule_interval = '@daily',
    start_date = days_ago(2)
) as dag:


    template_command_1 = dedent(
        """
        echo This command will SUCCEED
        exit 0
        """
    )
    template_command_2 = dedent(
        """
        echo This command will FAIL
        exit -1
        """
    )


    sleep_task = BashOperator(
        task_id = 'sleep_bash',
        bash_command = 'sleep 5',
        trigger_rule='all_success'
    )
    socorro_task = BashOperator(
        task_id = 'socorro_bash',
        bash_command = 'echo Oh no! Abysmal failure.',
        trigger_rule = 'all_done' # This task will run whether the upstream fails or succeeds
    )
    successful_task = BashOperator(
        task_id = 'successful_bash',
        bash_command = template_command_1
    )
    failed_task = BashOperator(
        task_id = 'failed_bash',
        on_failure_callback = socorro_task,
        on_success_callback = sleep_task,
        bash_command = template_command_2,
        trigger_rule = 'all_done'
    )
    
    # def branch_logic(ti):
    #     xcom_value = ti.xcom_pull(task_ids='failed_bash')
    #     if int(xcom_value)

    # branch_task = BranchPythonOperator(
    #     task_id = 'branch_on_bash_attempt',
    #     python_callable = branch_logic
    # )

    successful_task >> failed_task >> [sleep_task, socorro_task]
    # socorro_task.set_upstream(failed_task)