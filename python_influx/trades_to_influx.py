import json
import os
from Coinbase import RabbitMQ, Trade, CoinbaseInfluxDBClient

rabbit = RabbitMQ()
result = rabbit.create_queue(exclusive=True)
queue_name = result.method.queue
rabbit.bind_to_trades(queue_name)

client = CoinbaseInfluxDBClient(host=os.getenv("INFLUXDB_HOST", "influxdb"))


def callback(ch, method, props, body):
    data = json.loads(body)
    trade = Trade.from_dict(data)
    point = client.create_point(trade.trade_id, trade.side,
                        trade.size_scale, trade.size_unscaled,
                        trade.price_scale, trade.price_unscaled,
                        trade.time)
    client.write(point)


rabbit.channel.basic_consume(
    queue=queue_name, on_message_callback=callback
)

rabbit.channel.start_consuming()