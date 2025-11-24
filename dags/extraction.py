from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import datetime as dt
import pandas as pd
import json
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text
from requests.exceptions import HTTPError

def get_from_coingecko(currency: str, date: str) -> pd.DataFrame:
    """Intenta obtener precio histórico desde CoinGecko."""
    url = f"https://api.coingecko.com/api/v3/coins/{currency}/history"
    params = {"date": date, "localization": "false"}
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    data = response.json()
    return pd.DataFrame({
        "usd": [data["market_data"]["current_price"]["usd"]],
        "cripto": [currency]
    })


def get_from_freecrypto(currency: str) -> pd.DataFrame:
    """Fallback: obtiene precio desde FreeCryptoAPI."""
    transformation = {"bitcoin": "BTC", "ethereum": "ETH", "tether": "XAUT", "solana": "SOL"}
    currency_symbol = transformation[currency]
    url = f"https://api.freecryptoapi.com/v1/getData?symbol={currency_symbol}"
    headers = {
        "accept": "*/*",
        "Authorization": f"Bearer {os.getenv('API_KEY')}"
    }
    response = requests.get(url, headers=headers, timeout=10)
    response.raise_for_status()
    data = response.json()
    return pd.DataFrame({
        "usd": [data["symbols"][0]["last"]],
        "cripto": [currency]
    })


def get_prices(execution_date: str) -> pd.DataFrame:
    """
    Gets the prices of the relevant crypto currencies of a certain date and loads them to Redshift.
    """
    load_dotenv()
    print(execution_date)
    execution_date_dt = datetime.strptime(execution_date, "%Y-%m-%d")
    date_str = execution_date_dt.strftime("%d-%m-%Y")

    with open("./dags/currencies_to_extract.json", "r") as f:
        currencies = json.load(f)["currencies"]

    df_price = pd.DataFrame()

    for currency in currencies:
        try:
            df_new = get_from_coingecko(currency, date_str)
        except HTTPError as e:
            print(f"CoinGecko falló para {currency}: {e}. Usando FreeCryptoAPI...")
            df_new = get_from_freecrypto(currency)
        except Exception as e:
            print(f"Error inesperado en CoinGecko para {currency}: {e}. Usando FreeCryptoAPI...")
            df_new = get_from_freecrypto(currency)

        df_price = pd.concat([df_price, df_new], ignore_index=True)

    df_price["date"] = execution_date_dt

    engine = create_engine(os.getenv("REDSHIFT"))
    with engine.connect() as conn:
        for _, row in df_price.iterrows():
            conn.execute(
                text("INSERT INTO DAILY_CRIPTO_PRICES (usd, cripto, date) VALUES (:usd, :cripto, :date)"),
                {"usd": float(row.usd), "cripto": row.cripto, "date": row.date}
            )

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