import datetime, pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

KST = pendulum.timezone("Asia/Seoul")
default_args = {
    'owner': 'airflow_user',
    #'start_date': days_ago(1),
    'start_date': datetime.datetime(2022, 3, 4, tzinfo=KST),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5)
    }

dag = DAG(
    dag_id='get_Data',
    default_args=default_args,
    schedule_interval='30 16 * * *',  # mm hh
    catchup=False,
    )

stock = BashOperator(task_id='get_stock',
                     bash_command='python3 /home/ubuntu/stock_and_forum/src/data/stock.py',
                     dag=dag)
forum = BashOperator(task_id='get_forum',
                     bash_command='python3 /home/ubuntu/stock_and_forum/src/data/forum.py',
                     dag=dag)

stock >> forum

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