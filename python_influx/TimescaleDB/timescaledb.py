import psycopg2
import os
from Coinbase import Trade

TIMESCALE_HOST = "TIMESCALE_HOST"


class TimescaleDB:
    def __init__(self):
        self.connection = psycopg2.connect(host=os.getenv(TIMESCALE_HOST, "timescaledb"), port="5432",
                                           database="coinbase", password="bitcoin", user="postgres")
        self.cursor = self.connection.cursor()

    def insert(self, trades):
        if not isinstance(trades, list):
            trades = [trades]

        SQL = "INSERT INTO BTCEUR (time, trade_id, side, size, price) VALUES(%s, %s, %s, %s, %s)"
        data = [(trade.time, trade.trade_id, trade.side, trade.size_decimal(), trade.price_decimal()) for trade in trades]
        self.cursor.executemany(SQL, data)
        self.connection.commit()

    def get_id(self, trade_id):
        SQL = "SELECT time, trade_id, side, size, price from BTCEUR WHERE trade_id = %s"
        self.cursor.execute(SQL, (trade_id,))
        result = self.cursor.fetchone()
        return Trade.from_attributes(result[1], result[2], result[3], result[4], result[0])

    def min_id(self):
        SQL = "SELECT trade_id from BTCEUR ORDER BY trade_id ASC LIMIT 1"
        self.cursor.execute(SQL)
        result = self.cursor.fetchone()
        self.cursor.fetchall()
        return result[0]
