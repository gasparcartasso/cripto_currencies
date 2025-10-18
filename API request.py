import requests

url = "https://api.coingecko.com/api/v3/simple/price"
params = {
    "ids": "bitcoin,ethereum",
    "vs_currencies": "usd"
}

response = requests.get(url, params=params)
data = response.json()

print("Bitcoin Price (USD):", data["bitcoin"]["usd"])
print("Ethereum Price (USD):", data["ethereum"]["usd"])