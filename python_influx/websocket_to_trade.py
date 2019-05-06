import json
from Coinbase import RabbitMQ, Trade

rabbit = RabbitMQ()
result = rabbit.create_queue(exclusive=True)
queue_name = result.method.queue
rabbit.bind_to_coinbase_websocket(queue_name, "match")


def callback(ch, method, props, body):
    data = json.loads(body)
    trade = Trade.from_attributes(data["trade_id"], data["side"], data["size"], data["price"], data["time"])
    msg = json.dumps(trade.to_dict())
    rabbit.publish_to_trades(msg)


rabbit.channel.basic_consume(
    queue=queue_name, on_message_callback=callback, auto_ack=True
)

rabbit.channel.start_consuming()


