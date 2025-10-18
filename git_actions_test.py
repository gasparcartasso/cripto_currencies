from unittest.mock import patch
from API_request import get_bitcoin_price_2decimals

@patch("API_request.requests.get")
def test_get_bitcoin_price(mock_get):
    mock_get.return_value.json.return_value = {'market_data':{'current_price':{'usd':1.54563}}}
    assert isinstance(get_bitcoin_price_2decimals(),float)