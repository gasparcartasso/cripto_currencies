from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import datetime as dt
import pandas as pd
import json
from datetime import datetime
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text

def get_prices(execution_date):
    """
    Gets the prices of the relevant cripto currencies of a certain date and returns their values and function loads the data to redshift

    Args:
        execution_date (str): Date of execution. Format: "YYYY-MM-DD".

    Returns:
        Pd.DataFrame: Prices for that execution date.

    Raises:
        None.

    Example:
        >>> calculate_area(2025-10-20)
        0  91363.278387   bitcoin 2025-10-20
        1   3017.748361  ethereum 2025-10-20
        2      0.999136    tether 2025-10-20
        3    136.434705    solana 2025-10-20
    """
    load_dotenv()
    print(execution_date)
    execution_date = datetime.strptime(execution_date, "%Y-%m-%d")
    date = execution_date.strftime("%d-%m-%Y")
    with open('./dags/currencies_to_extract.json','r') as f:
        currencies=json.load(f)['currencies']
    df_price = pd.DataFrame()
    for currency in currencies:
        try:
            url = f"https://api.coingecko.com/api/v3/coins/{currency}/history"
            params = {
            "date": f"{date}",  # Format: DD-MM-YYYY
            "localization": "false"
            }
            response = requests.get(url, params=params)
            data = response.json()
            df_price = pd.concat([df_price,pd.DataFrame({'usd':data['market_data']['current_price']['usd'],'cripto':[currency]})],ignore_index=True)
        except:
            transformation = {"bitcoin":"BTC","ethereum":"ETH","tether":"XAUT","solana":"SOL"}
            currency_symbol = transformation[currency]
            url = f"https://api.freecryptoapi.com/v1/getData?symbol={currency_symbol}"
            headers = {
            "accept": "*/*",
            "Authorization": f"Bearer {os.getenv("API_KEY")}"
            }
            response = requests.get(url, headers=headers)
            data = response.json()
            df_price = pd.concat([df_price,pd.DataFrame({'usd':data['symbols'][0]['last'],'cripto':[currency]})],ignore_index=True)
    df_price['date']=execution_date
    engine = create_engine(os.getenv("REDSHIFT"))
    with engine.connect() as conn:
        for index, row in df_price.iterrows():
            conn.execute(
                text("INSERT INTO DAILY_CRIPTO_PRICES (usd, cripto,date) VALUES (:usd, :cripto,:date)"),
                {"usd": float(row.usd), "cripto":row.cripto,'date':row.date}
                )
        conn.close()
    return df_price

dag = DAG(dag_id="extraction",
         start_date=datetime(2025,10, 20),
         schedule="@daily",
         catchup=True)

extract_load = PythonOperator(
        task_id="currency_extraction",
        python_callable=get_prices,
        op_kwargs={"execution_date": "{{ ds }}"},
        dag=dag
        )

extract_load