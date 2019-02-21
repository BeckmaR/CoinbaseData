from influxdb import InfluxDBClient
from decimal import Decimal as D
import dateutil.parser as parser


class CoinbaseInfluxDBClient:
    def __init__(self, host):
        self.client = InfluxDBClient(host=host, database="coinbase")
        self.client.ping()

    def from_match(self, message, product_id=None, use_auto_inc=False):
        trade_id = int(message["trade_id"])
        side = message["side"]
        size = D(message["size"])
        price = D(message["price"])
        if product_id is None:
            product_id = message["product_id"]
        time = message["time"]
        ms = self._rfc3339_to_ms(message["time"])
        size_i, size_s = self._split_decimal(size)
        price_i, price_s = self._split_decimal(price)

        # Calculate a tag based on the trade_id.
        # Because influxDB doesn't like having too many unique tags,
        # trade_id should not be used as a tag.
        # Instead, tag % 1000 can be used to retreive a trade sufficiently quickly,
        # while not producing too many series.

        tags = {
            "uid": trade_id % 1000,
            "side": side
        }

        # Take the current number of trades at this timestamp for another tag.
        # This should eliminate all duplicates.

        if use_auto_inc:
            trades = list(self.get_trades_at_time(product_id, time))
            tags["autoinc"] = len(trades)

        point = {
                "measurement": product_id,
                "tags": tags,
                "fields": {
                    "size_unscaled": size_i,
                    "size_scale": size_s,
                    "price_unscaled": price_i,
                    "price_scale": price_s,
                    "trade_id": trade_id
                },
                "time": ms
        }

        return point

    def write(self, data):
        if not isinstance(data, list):
            data = [data]
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

    def get_max_trade_id(self, product_id):
        query = 'SELECT max("trade_id") FROM "{product_id}"'.format(product_id=product_id)
        rs = self.query(query)
        for t in rs.get_points(measurement=product_id):
            return t['max']

    def get_trades_at_time(self, product_id, time):
        query = 'SELECT * FROM "BTC-EUR" WHERE time={0}'.format("'" + str(time) + "'")
        rs = self.client.query(query)
        return rs.get_points(measurement=product_id)

    def has_ids_missing(self, product_id):
        query = 'SELECT max("trade_id") - min("trade_id") - count("trade_id") FROM "BTC-EUR"'
        rs = self.client.query(query)
        for t in rs.get_points(measurement=product_id):
            print("'max_min_count'=" + str(t["max_min_count"]))
            return t["max_min_count"] != 1

    def get_points(self, query, measurement, **kwargs):
        response = self.query(query.format(**kwargs))
        return list(response.get_points(measurement=measurement))

    def _rfc3339_to_ms(self, timestring):
        dt = parser.parse(timestring)
        ts = dt.timestamp()
        s = int(ts)
        ms = s * 1000 + dt.microsecond / 1000
        return int(ms)