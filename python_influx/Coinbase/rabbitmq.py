import pika

COINBASE_WEBSOCKET = "coinbase-websocket"


class RabbitMQ:
    def __init__(self, host):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host)
        )
        self.channel = self.connection.channel()

    def declare_coinbase_websocket(self):
        self.channel.exchange_declare(COINBASE_WEBSOCKET, exchange_type="direct")

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