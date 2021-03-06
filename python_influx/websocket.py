import os
import asyncio
import websockets
import json
import pika
from Coinbase import influx, CoinbaseInfluxDBClient, RabbitMQ

coinbase_api_ws = "wss://ws-feed.pro.coinbase.com"

products = ["BTC-EUR"]

#influx_client = CoinbaseInfluxDBClient(host=os.getenv("INFLUXDB_HOST", "influxdb"))

rabbitmq = RabbitMQ()
rabbitmq.declare_coinbase_websocket()


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
    message_json = json.loads(message)
    rabbitmq.publish_to_coinbase_websocket(message, message_json["type"])
    # message = json.loads(message)
    # if message["type"] == "match":
    #     consume_match(message)
    # elif message["type"] == "heartbeat":
    #     consume_heartbeat(message)


def consume_match(message):
    point = influx_client.from_matches([message])
    print(point)
    influx_client.write(point)


def consume_heartbeat(message):
    print(message)


asyncio.get_event_loop().run_until_complete(subscribe())
