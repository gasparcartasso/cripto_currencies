from unittest.mock import patch
import pandas as pd
from API_request import get_bitcoin_price_2decimals_usd, get_max_52w_rolling

@patch("API_request.requests.get")
def test_get_bitcoin_price(mock_get):
    mock_get.return_value.json.return_value = {'market_data':{'current_price':{'usd':1.54563}}}
    assert isinstance(get_bitcoin_price_2decimals_usd(),float)

def test_get_max_52_week_rolling():
    history = pd.DataFrame({'usd':[1.352,1.312542463,21414.124,1243],'cripto':['bitcoin','bitcoin','bitcoin','bitcoin'],'date':['23-10-2025','24-10-2025','25-10-2025','26-10-2025']})
    result = get_max_52w_rolling(history)
    assert result['usd_max_year'].max==1243