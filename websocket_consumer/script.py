from influxdb import InfluxDBClient
import asyncio
import websockets
import json
from decimal import Decimal as D

coinbase_api_ws = "wss://ws-feed.pro.coinbase.com"

products = ["BTC-EUR"]

influx_client = InfluxDBClient(host="influxdb", database="coinbase")

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
    message = json.loads(message)
    if message["type"] == "match":
        consume_match(message)


def consume_match(message):
    trade_id = message["trade_id"]
    side = message["side"]
    size = D(message["size"])
    price = D(message["price"])
    product_id = message["product_id"]
    time = message["time"]

    size_i, size_s = split_decimal(size)
    price_i, price_s = split_decimal(price)

    data = [
        {
            "measurement": product_id,
            "tags": {
                "trade_id": trade_id,
                "side": side
            },
            "fields": {
                "size_unscaled": size_i,
                "size_scale": size_s,
                "price_unscaled": price_i,
                "price_scale": price_s
            },
            "time": time
        }
    ]

    influx_client.write_points(data)


def split_decimal(dec):
    t = dec.as_tuple()
    i = 0
    for d in t[1]:
        i = i * 10 + d
    return i, t[2]


asyncio.get_event_loop().run_until_complete(subscribe())
