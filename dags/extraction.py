from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello_world():
    print("Hello Airflow!")


dag = DAG(dag_id="hello_airflow",
         start_date=datetime(2023, 1, 1),
         schedule_interval="@daily",
         catchup=False)

task = PythonOperator(
        task_id="say_hello",
        python_callable=hello_world,
        dag=dag
    )

task