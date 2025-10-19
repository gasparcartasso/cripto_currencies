#%%
import requests
def get_bitcoin_price():
    import requests
    import datetime as dt
    import pandas as pd
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
    import requests
    import datetime as dt
    date = dt.date.today().strftime("%d-%m-%Y")
    url = "https://api.coingecko.com/api/v3/coins/bitcoin/history"
    params = {
        "date": f"{date}",  # Format: DD-MM-YYYY
        "localization": "false"
    }

    response = requests.get(url, params=params)
    data = response.json()
    return round(data['market_data']['current_price']['usd'],2)