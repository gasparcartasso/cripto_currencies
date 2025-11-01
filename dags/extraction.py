from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from API_request import get_prices


dag = DAG(dag_id="extraction",
         start_date=datetime(2025,10, 20),
         schedule="@daily",
         catchup=True)

task = PythonOperator(
        task_id="currency_extraction",
        python_callable=get_prices,
        op_kwargs={"execution_date": "{{ ds }}"},
        dag=dag
    )

task