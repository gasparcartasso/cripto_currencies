#%%
import requests
import datetime as dt
import pandas as pd
import json
from datetime import datetime
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text
# Resolve the parent directory of this .py file
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.abspath(os.path.join(BASE_DIR, os.pardir))

# Load the .env file from the parent directory
dotenv_path = os.path.join(PARENT_DIR, ".env")
load_dotenv(dotenv_path=dotenv_path)

#%%
def get_bitcoin_price_2decimals_usd():
    date = dt.date.today().strftime("%d-%m-%Y")
    url = "https://api.coingecko.com/api/v3/coins/bitcoin/history"
    params = {
        "date": f"{date}",  # Format: DD-MM-YYYY
        "localization": "false"
    }

    response = requests.get(url, params=params)
    data = response.json()
    return round(data['market_data']['current_price']['usd'],2)

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
        >>> get_prices(2025-10-20)
        0  91363.278387   bitcoin 2025-10-20
        1   3017.748361  ethereum 2025-10-20
        2      0.999136    tether 2025-10-20
        3    136.434705    solana 2025-10-20
    """
    print(execution_date)
    execution_date = datetime.strptime(execution_date, "%Y-%m-%d")
    date = execution_date.strftime("%d-%m-%Y")
    with open('currencies_to_extract.json','r') as f:
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
    df_price['date']=date
    engine = create_engine(os.getenv("REDSHIFT"))
    with engine.connect() as conn:
        for index, row in df_price.iterrows():
            conn.execute(
                text("INSERT INTO DAILY_CRIPTO_PRICES (usd, cripto,date) VALUES (:usd, :cripto,:date)"),
                {"usd": row.usd, "cripto":row.cripto,'date':row.date}
                )
        conn.close()
    return df_price

def get_max_4w_rolling(history):
    """
    This function gets a 52 week rolling max of the currency

    Args:
        history (str): max by date of each cripto currency.

    Returns:
        pd.DataFrame: max by date of each cripto currency.

    Raises:
        None.

    Example:
        >>> get_max_4w_rolling(history)
        0  91363.278387   bitcoin 2025-10-20
        1   3017.748361  ethereum 2025-10-20
        2      0.999136    tether 2025-10-20
        3    136.434705    solana 2025-10-20
    """
    currencies = history['cripto'].unique()
    price_max=pd.DataFrame()
    for currency in currencies:
        price_max = pd.concat([price_max,pd.DataFrame({'max_4week':history[history['cripto']==currency].sort_values(by='date',ascending=True)['usd'].rolling(window=28,min_periods=1).max(),'cripto':history[history['cripto']==currency].sort_values(by='date',ascending=True)['cripto'],'date':history[history['cripto']==currency].sort_values(by='date',ascending=True)['date']})],ignore_index=True)
    return price_max

def get_avg_4w_rolling(history):
    """
    This function gets a 52 week rolling mean of the currency

    Args:
        history (str): mean by date of each cripto currency.

    Returns:
        pd.DataFrame: mean by date of each cripto currency.

    Raises:
        None.

    Example:
        >>> get_mean_4w_rolling(history)
        0  91363.278387   bitcoin 2025-10-20
        1   3017.748361  ethereum 2025-10-20
        2      0.999136    tether 2025-10-20
        3    136.434705    solana 2025-10-20
    """
    currencies = history['cripto'].unique()
    price_avg=pd.DataFrame()
    for currency in currencies:
        price_avg = pd.concat([price_avg,pd.DataFrame({'mean_4week':history[history['cripto']==currency].sort_values(by='date',ascending=True)['usd'].rolling(window=28,min_periods=1).mean(),'cripto':history[history['cripto']==currency].sort_values(by='date',ascending=True)['cripto'],'date':history[history['cripto']==currency].sort_values(by='date',ascending=True)['date']})],ignore_index=True)
    return price_avg

def get_min_4w_rolling(history):
    """
    This function gets a 52 week rolling min of the currency

    Args:
        history (str): min by date of each cripto currency.

    Returns:
        pd.DataFrame: min by date of each cripto currency.

    Raises:
        None.

    Example:
        >>> get_min_4w_rolling(history)
        0  91363.278387   bitcoin 2025-10-20
        1   3017.748361  ethereum 2025-10-20
        2      0.999136    tether 2025-10-20
        3    136.434705    solana 2025-10-20
    """
    currencies = history['cripto'].unique()
    price_min=pd.DataFrame()
    for currency in currencies:
        price_min = pd.concat([price_min,pd.DataFrame({'min_4week':history[history['cripto']==currency].sort_values(by='date',ascending=True)['usd'].rolling(window=28,min_periods=1).min(),'cripto':history[history['cripto']==currency].sort_values(by='date',ascending=True)['cripto'],'date':history[history['cripto']==currency].sort_values(by='date',ascending=True)['date']})],ignore_index=True)
    return price_min

def load_data_to_redshift(data):
    """
    This function loads the data to redshift

    Args:
        data (str): data to load.

    Returns:
        None.

    Raises:
        None.
    """
    engine = create_engine(os.getenv("REDSHIFT"))
    with engine.connect() as conn:
        for index, row in data.iterrows():
            conn.execute(
                text("INSERT INTO DAILY_CRIPTO_PRICES (usd, cripto,date) VALUES (:usd, :cripto,:date)"),
                {"usd": row.usd, "cripto":row.cripto,'date':row.date}
                )
        conn.close()