import sys, os
sys.path.append((os.path.dirname(__file__)))
from util import common

import datetime, pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

src_folder = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
config = common.Config()
info = config.get("EMAIL")

KST = pendulum.timezone("Asia/Seoul")
default_args = {
    'owner': 'airflow_user',
    'start_date': days_ago(1),
    #'start_date': datetime.datetime(2022, 3, 4, tzinfo=KST),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'email_on_failure': True,
    'email': [info['email']],
    }

dag = DAG(
    dag_id='get_data',
    default_args=default_args,
    schedule_interval='20 7 * * *',  # mm hh
    catchup=False,
    )

stock = BashOperator(task_id='get_stock',
                     bash_command='python3 %s/data/stock.py' % src_folder,
                     dag=dag)
forum = BashOperator(task_id='get_forum',
                     bash_command='python3 %s/data/forum.py' % src_folder,
                     dag=dag)

stock >> forum