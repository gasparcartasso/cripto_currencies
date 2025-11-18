from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
import os
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
load_dotenv()
from API_request import get_avg_4w_rolling, get_max_4w_rolling, get_min_4w_rolling
from sqlalchemy import create_engine, text

def load_fact_table():
    """
    This function loads the fact table with the relevant data every time it is called will drop and recreate the fact table
    Return: None
    """
    conn = psycopg2.connect(
    dbname=os.getenv('REDSHIFT_SCHEMA'),
    user=os.getenv('REDSHIFT_USER'),
    password=os.getenv('REDSHIFT_PASSWORD'),
    host=os.getenv('REDSHIFT_HOST'),
    port=5439
    )
    cur = conn.cursor()
    create_table_sql = """
    DROP TABLE IF EXISTS FACT_CRIPTO_PRICES;

    CREATE TABLE FACT_CRIPTO_PRICES (
        usd FLOAT,
        mean_4week FLOAT,
        max_4week FLOAT,
        min_4week FLOAT,
        cripto VARCHAR(100),
        date TIMESTAMP
    );
    """
    cur.execute(create_table_sql)
    conn.commit()
    df = pd.read_sql("SELECT * FROM DAILY_CRIPTO_PRICES where date>= '2025-09-01' ;", conn)
    df.drop_duplicates(subset=['cripto','date'],inplace=True)
    data = get_min_4w_rolling(df).merge(get_avg_4w_rolling(df),on=['cripto','date']).merge(get_max_4w_rolling(df),on=['cripto','date']).merge(df[['cripto','date','usd']],on=['cripto','date'])
    engine = create_engine(os.getenv("REDSHIFT"))
    for index, row in data.iterrows():
        with engine.connect() as conn:
            conn.execute(
            text("INSERT INTO FACT_CRIPTO_PRICES (usd,mean_4week,max_4week,min_4week,cripto,date) VALUES (:usd,:mean_4week,:max_4week,:min_4week,:cripto,:date)"),
            {"usd": row.usd,"mean_4week":row.mean_4week,'max_4week':row.max_4week,'min_4week':row.min_4week, "cripto":row.cripto,'date':row.date}
            )
            conn.close()
        print(index)

dag = DAG(dag_id="facttable",
         start_date=datetime(2025,10, 20),
         schedule="@daily",
         catchup=False)

load_create_fact_table = PythonOperator(
        task_id="load_create_fact_table",
        python_callable=load_fact_table,
        dag=dag
        )

load_create_fact_table