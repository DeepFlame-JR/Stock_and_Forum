import datetime, pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

KST = pendulum.timezone("Asia/Seoul")
default_args = {
    'owner': 'airflow_user',
    'start_date': days_ago(1),
    # 'start_date': datetime.datetime(2022, 2, 26, tzinfo=KST),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5)
    }

dag = DAG(
    dag_id='example_dag',
    default_args=default_args,
    schedule_interval='*/1 * * * *', #mm hh
    catchup=False,
    )

test1 = BashOperator(task_id='date', bash_command='date', dag=dag)
test2 = BashOperator(task_id='whoami', bash_command='whoami', dag=dag)

test1 >> test2