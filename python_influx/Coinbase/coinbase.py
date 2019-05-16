import requests
from .trade import Trade

class CoinbaseRESTService:
    def __init__(self):
        self.api_url = 'https://api.pro.coinbase.com'
        self.trade_endpoint = '/products/{product_id}/trades'

    def get_id(self, product_id, trade_id):
        response = requests.get(self.api_url + self.trade_endpoint.format(product_id=product_id), params={"after": trade_id+1, "limit": 100})
        print(response.url)
        data = response.json()
        trades = [Trade.from_attributes(
                    t["trade_id"],
                    t["side"],
                    t["size"],
                    t["price"],
                    t["time"]
                ) for t in data]
        return trades
