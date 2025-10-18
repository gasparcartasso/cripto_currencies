import requests
def get_bitcoin_price():
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
    return data['market_data']['current_price']['usd']

def get_bitcoin_price_2decimals():
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