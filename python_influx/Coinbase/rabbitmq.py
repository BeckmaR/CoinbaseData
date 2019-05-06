import pika
import os
COINBASE_WEBSOCKET = "coinbase-websocket"
TRADES = "trades"


class RabbitMQ:
    def __init__(self, host=None):
        if host is None:
            host = os.getenv("RABBITMQ_HOST", "rabbit")
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host)
        )
        self.channel = self.connection.channel()

    def declare_coinbase_websocket(self):
        self.channel.exchange_declare(COINBASE_WEBSOCKET, exchange_type="direct")

    def declare_trades(self):
        self.channel.exchange_declare(TRADES, exchange_type="fanout")

    def create_queue(self,
                      queue,
                      passive=False,
                      durable=False,
                      exclusive=False,
                      auto_delete=False,
                      arguments=None):
        return self.channel.queue_declare(queue,
                                          passive=passive,
                                          durable=durable,
                                          exclusive=exclusive,
                                          auto_delete=auto_delete,
                                          arguments=arguments)

    def bind_to_coinbase_websocket(self, queue, routing_key):
        self.channel.queue_bind(
            exchange=COINBASE_WEBSOCKET, queue=queue, routing_key=routing_key
        )

    def publish_to_coinbase_websocket(self, message, routing_key):
        self.channel.basic_publish(
            exchange=COINBASE_WEBSOCKET, routing_key=routing_key, body=message
        )

    def bind_to_trades(self, queue):
        self.channel.queue_bind(
            exchange=TRADES, queue=queue
        )

    def publish_to_trades(self, message):
        self.channel.basic_publish(
            exchange=TRADES, routing_key="", body=message
        )