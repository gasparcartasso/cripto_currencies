from unittest.mock import patch
import pandas as pd
from dags.API_request import get_bitcoin_price_2decimals_usd, get_max_4w_rolling,load_data_to_redshift
from unittest.mock import MagicMock
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

@patch("API_request.requests.get")
def test_get_bitcoin_price(mock_get):
    mock_get.return_value.json.return_value = {'market_data':{'current_price':{'usd':1.54563}}}
    assert get_bitcoin_price_2decimals_usd()==1.54

def test_get_max_4_week_rolling():
    history = pd.DataFrame({'usd':[1.352,1.312542463,21414.124,1243],'cripto':['bitcoin','bitcoin','bitcoin','bitcoin'],'date':['23-10-2025','24-10-2025','25-10-2025','26-10-2025']})
    result = get_max_4w_rolling(history)
    assert result['usd_max_year'].max==1243

def test_save_to_redshift_calls_client():
    mock_client = MagicMock()
    record = {"usd": 123.23, "cripto": "bitcoin", "date": '2025-10-11'}
    load_data_to_redshift(mock_client, record)
    mock_client.execute.assert_called_once()
