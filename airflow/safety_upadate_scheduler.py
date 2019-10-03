from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


schedule_interval = timedelta(days=2)

default_args = {
    'owner': 'LIBO_Song',
    'depends_on_past': False,
    'start_date': datetime.now() - schedule_interval,
    'email': ['libosongtang@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'batch_scheduler',
    default_args=default_args,
    description='DAG for the safety info update Job',
    schedule_interval=schedule_interval)


task = BashOperator(
    task_id='run_batch_job',
    bash_command='cd /home/ubuntu/project/tonebnb/batching/ ; ./run.sh airflow_update_safetyinfo.py',
    dag=dag)

