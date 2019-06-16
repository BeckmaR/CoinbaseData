import psycopg2
import os
from Coinbase import RabbitMQ, Trade
import json

TIMESCALE_HOST = "TIMESCALEDB_HOST"

connection = psycopg2.connect(host=os.getenv(TIMESCALE_HOST, "timescaledb"), port="5432", database="coinbase", password="bitcoin", user="postgres")

cursor = connection.cursor()
print(connection.get_dsn_parameters())

cursor.execute("SELECT version();")
record = cursor.fetchone()
print("You are connected to - ", record,"\n")

SQL = "INSERT INTO BTCEUR (time, trade_id, side, size, price) VALUES(%s, %s, %s, %s, %s)"

rabbit = RabbitMQ()
rabbit.declare_trades()
result = rabbit.create_queue(exclusive=True)
queue_name = result.method.queue
rabbit.bind_to_trades(queue_name)

def callback(ch, method, props, body):
    data = json.loads(body)
    trade = Trade.from_dict(data)
    cursor.execute(SQL, (trade.time, trade.trade_id, trade.side, trade.size_decimal(), trade.price_decimal()))
    connection.commit()
    ch.basic_ack(delivery_tag=method.delivery_tag)

rabbit.channel.basic_consume(
    queue=queue_name, on_message_callback=callback
)

rabbit.channel.start_consuming()




