import datetime,pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

KST = pendulum.timezone("Asia/Seoul")
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    # 'start_date': datetime.datetime(2022, 2, 26, tzinfo=KST),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5)
    }

dag = DAG(
    dag_id='first_dag3',
    default_args=default_args,
    schedule_interval='*/1 * * * *', #mm hh
    catchup=False,
    )

test1 = BashOperator(task_id='test1', bash_command='python /home/toy/src/airflow/test1.py', dag=dag)
test2 = BashOperator(task_id='test2', bash_command='python /home/toy/src/airflow/test2.py', dag=dag)

test1 >> test2