from influxdb import InfluxDBClient
from decimal import Decimal as D


class CoinbaseInfluxDBClient:
    def __init__(self, host):
        self.client = InfluxDBClient(host=host, database="coinbase")
        self.client.ping()

    def from_match(self, message, product_id=None):
        trade_id = int(message["trade_id"])
        side = message["side"]
        size = D(message["size"])
        price = D(message["price"])
        if product_id is None:
            product_id = message["product_id"]
        time = message["time"]

        # Calculate a tag based on the trade_id.
        # Because influxDB doesn't like having too many unique tags,
        # trade_id should not be used as a tag.
        # But because multiple trades can happen at the same time,
        # trade_id % 100 is used as a tag. This means at most 1000
        # different tags can occur, and 100 trades can happen at the same time
        # without overwriting happening.

        trade_id_tag = trade_id % 100

        size_i, size_s = self._split_decimal(size)
        price_i, price_s = self._split_decimal(price)

        data = [
            {
                "measurement": product_id,
                "tags": {
                    "uid": trade_id_tag,
                    "side": side
                },
                "fields": {
                    "size_unscaled": size_i,
                    "size_scale": size_s,
                    "price_unscaled": price_i,
                    "price_scale": price_s,
                    "trade_id": trade_id
                },
                "time": time
            }
        ]
        # print(str(data))
        self.client.write_points(data, time_precision='ms')

    def _split_decimal(self, dec):
        t = dec.as_tuple()
        i = 0
        for d in t[1]:
            i = i * 10 + d
        return i, t[2]

    def query(self, query):
        return self.client.query(query)

    def get_min_trade_id(self, product_id):
        query = 'SELECT min("trade_id") FROM "{product_id}"'.format(product_id=product_id)
        rs = self.query(query)
        for t in rs.get_points(measurement=product_id):
            return t['min']