import sys, os, platform, time
if 'Windows' not in platform.platform():
    os.environ['TZ'] = 'Asia/Seoul'
    time.tzset()

src_folder = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))
sys.path.append(src_folder)

import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 2,
    'retry_delay': datetime.timedelta(minutes=5),
    }

dag = DAG(
    dag_id='get_data',
    default_args=default_args,
    schedule_interval='0 11 * * *',  # mm hh (매일 20:00에 실행)
    catchup=False,
    )

# DAG 작성
def execute_forum(start, end, port, **kwargs):
    from data import forum
    forum.main_get_forum(start, end, port)
    return "Executor End"

stock = BashOperator(task_id='get_stock',
                     bash_command='python3 %s/data/stock.py' % src_folder,
                     dag=dag)

etl = BashOperator(task_id='ETL',
                   bash_command='python3 %s/dw/spark_run.py' % src_folder,
                   dag=dag)

forum_tasks = {}
for i, f in enumerate(["f1", "f2", "f3", "f4", "f5"]):
    task = PythonOperator(
        task_id=f"forum_{i+1}",
        python_callable=execute_forum,
        op_kwargs={"start":i*11, "end":min(50,(i+1)*11), "port":4444+i},
        dag=dag,
    )
    forum_tasks[f] = task

stock >> forum_tasks["f1"] >> forum_tasks["f5"] >> etl
stock >> forum_tasks["f2"] >> forum_tasks["f3"] >> forum_tasks["f4"] >> etl
