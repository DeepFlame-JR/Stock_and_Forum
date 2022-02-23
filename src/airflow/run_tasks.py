import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2022, 2, 23),
    'retries': 2,
    'retry_delay': datetime.timedelta(minutes=5)
    }

dag = DAG(
    '',
    default_args=default_args,
    schedule_interval='*/15 * * * *',
    )

test1 = BashOperator(task_id='test1', bash_command='python ~/Stock_and_Forum/src/airflow/test1.py', dag=dag)
test2 = BashOperator(task_id='test2', bash_command='python ~/Stock_and_Forum/src/airflow/test2.py', dag=dag)

test1 >> test2

# db_stock = BashOperator(
#             task_id='db_stock',
#             bash_command='python ',
#             dag=dag
#             )
#
# db_forum = BashOperator(
#             task_id='db_forum',
#             bash_command='python ~/Stock_and_Forum/src/airflow',
#             dag=dag
#             )