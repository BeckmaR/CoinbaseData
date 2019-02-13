from influxdb import InfluxDBClient
import asyncio
import websockets
import json
import requests
from decimal import Decimal as D

coinbase_api_ws = "wss://ws-feed.pro.coinbase.com"
coinbase_api = "https://api.pro.coinbase.com"

products = ["BTC-EUR"]

influx_client = InfluxDBClient(host="influxdb", database="coinbase")

max_match_id = {}

async def subscribe():
    async with websockets.connect(coinbase_api_ws) as websocket:
        subscribe_object = {
            "type": "subscribe",
            "product_ids": products,
            "channels": ["matches"]
        }
        subscribe_s = json.dumps(subscribe_object)
        print("Send: " + subscribe_s)
        await websocket.send(subscribe_s)

        async for message in websocket:
            consume(message)


def consume(message):
    print(message)
    message = json.loads(message)
    if message["type"] == "match":
        consume_match(message)
    elif message["type"] == "heartbeat":
        consume_heartbeat(message)


def consume_match(message):
    trade_id = int(message["trade_id"])
    side = message["side"]
    size = D(message["size"])
    price = D(message["price"])
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

    max_match_id[product_id] = trade_id

    size_i, size_s = split_decimal(size)
    price_i, price_s = split_decimal(price)

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
    print(str(data))
    influx_client.write_points(data, time_precision='ms')


def consume_heartbeat(message):
    last_trade_id = message["last_trade_id"]
    product_id = message["product_id"]
    if not product_id in max_match_id or last_trade_id > max_match_id[product_id]:
        print("Loading missing from REST: [" + product_id + "] - " + str(last_trade_id))


def get_from_rest(product_id, trade_id):
    url = coinbase_api + "/products/" + product_id + "/trades"
    r = requests.get(url=url, params={"after": trade_id+1, "limit": 1})
    data = r.json()
    consume_match(data[0])

def split_decimal(dec):
    t = dec.as_tuple()
    i = 0
    for d in t[1]:
        i = i * 10 + d
    return i, t[2]


asyncio.get_event_loop().run_until_complete(subscribe())
