from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

from datetime import datetime

seven_days_ago = datetime.combine(datetime.today() - timedelta(7),
                                      datetime.min.time())

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': seven_days_ago,
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
  }

dag = DAG('proje', default_args=default_args,
          schedule_interval='* * * * *',
          catchup=False)

t1 = BashOperator(
        task_id='encode',
        bash_command='python3 /opt/airflow/dags/encode.py',
        dag=dag)

t2 = BashOperator(
        task_id='predict',
        bash_command='python3 /opt/airflow/dags/predict.py',
        dag=dag)
t1>>t2
