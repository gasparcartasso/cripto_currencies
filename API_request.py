#%%
import requests
import datetime as dt
import pandas as pd
def get_bitcoin_price():
    date = dt.date.today().strftime("%d-%m-%Y")
    url = "https://api.coingecko.com/api/v3/coins/bitcoin/history"
    params = {
        "date": f"{date}",  # Format: DD-MM-YYYY
        "localization": "false"
    }

    response = requests.get(url, params=params)
    data = response.json()
    df_price = pd.DataFrame([data['market_data']['current_price']])
    df_price['date']=date
    return df_price
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

def get_prices():
    """
    Gets the prices of the relevant cripto currencies of a certain date and returns their values.
    Returns (pd.DataFrame)
    """
    date = dt.date.today().strftime("%d-%m-%Y")
    currencies=['bitcoin','ethereum','tether','solana']
    df_price = pd.DataFrame()
    for currency in currencies:
        url = f"https://api.coingecko.com/api/v3/coins/{currency}/history"
        params = {
        "date": f"{date}",  # Format: DD-MM-YYYY
        "localization": "false"
        }
        response = requests.get(url, params=params)
        data = response.json()
        df_price = pd.concat([df_price,pd.DataFrame({'usd':data['market_data']['current_price']['usd'],'cripto':[currency]})],ignore_index=True)
    df_price['date']=date
    return df_price

def get_max_52w_rolling(history):
    """
    This function gets a 52 week rolling max of the currency
    Arg (pd.DataFrame): history to analyse
    Return (pd.DataFrame): max by date of each cripto currency
    """
    currencies = history['cripto'].unique()
    price_max=pd.DataFrame()
    for currency in currencies:
        price_max = pd.concat([price_max,pd.DataFrame({'usd_max_year':history[history['cripto']==currency].sort_values(by='date',ascending=True)['usd'].rolling(window=365,min_periods=1).max(),'cripto':history[history['cripto']==currency].sort_values(by='date',ascending=True)['cripto'],'date':history[history['cripto']==currency].sort_values(by='date',ascending=True)['date']})],ignore_index=True)
    return price_max

def get_avg_52w_rolling(history):
    """
    This function gets a 52 week rolling avg of the currency
    Arg (pd.DataFrame): history to analyse
    Return (pd.DataFrame): avg by date of each cripto currency
    """
    currencies = history['cripto'].unique()
    price_avg=pd.DataFrame()
    for currency in currencies:
        price_avg = pd.concat([price_avg,pd.DataFrame({'usd_avg_year':history[history['cripto']==currency].sort_values(by='date',ascending=True)['usd'].rolling(window=365,min_periods=1).mean(),'cripto':history[history['cripto']==currency].sort_values(by='date',ascending=True)['cripto'],'date':history[history['cripto']==currency].sort_values(by='date',ascending=True)['date']})],ignore_index=True)
    return price_avg

def get_min_52w_rolling(history):
    """
    This function gets a 52 week rolling min of the currency
    Arg (pd.DataFrame): history to analyse
    Return (pd.DataFrame): min by date of each cripto currency
    """
    currencies = history['cripto'].unique()
    price_min=pd.DataFrame()
    for currency in currencies:
        price_min = pd.concat([price_min,pd.DataFrame({'usd_min_year':history[history['cripto']==currency].sort_values(by='date',ascending=True)['usd'].rolling(window=365,min_periods=1).min(),'cripto':history[history['cripto']==currency].sort_values(by='date',ascending=True)['cripto'],'date':history[history['cripto']==currency].sort_values(by='date',ascending=True)['date']})],ignore_index=True)
    return price_min