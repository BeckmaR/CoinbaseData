import psycopg2
import os
from Coinbase import Trade

TIMESCALE_HOST = "TIMESCALE_HOST"


class TimescaleDB:
    def __init__(self, host=None):
        if host is None:
            host = os.getenv(TIMESCALE_HOST, "timescaledb")
        self.connection = psycopg2.connect(host=host, port="5432",
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

    def max_id(self):
        SQL = "SELECT trade_id from BTCEUR ORDER BY trade_id DESC LIMIT 1"
        self.cursor.execute(SQL)
        result = self.cursor.fetchone()
        self.cursor.fetchall()
        return result[0]

    def get_range(self, min_id, max_id):
        SQL = "SELECT time, trade_id, side, size, price from BTCEUR WHERE trade_id >= %s AND trade_id <= %s"
        self.cursor.execute(SQL, (min_id, max_id))
        result = self.cursor.fetchall()
        return [Trade.from_attributes(row[1], row[2], row[3], row[4], row[0]) for row in result]

    def delete(self, trade_or_id):
        if isinstance(trade_or_id, int):
            trade_id = trade_or_id
        elif isinstance(trade_or_id, Trade):
            trade_id = trade_or_id.trade_id
        SQL = "DELETE FROM BTCEUR where trade_id = %s"
        self.cursor.execute(SQL, (trade_id, ))
        result = self.cursor.fetchall()
        self.connection.commit()

    def get_max_validated(self):
        SQL = "SELECT max(max_validated) FROM BTCEUR_validated"
        self.cursor.execute(SQL)
        result = self.cursor.fetchall()
        return result[0][0]

    def set_max_validated(self, max_validated):
        SQL = "INSERT INTO BTCEUR_validated (max_validated) VALUES (%s)"
        self.cursor.execute(SQL, (max_validated,))
        self.connection.commit()

    def execute(self, SQL, arguments=None):
        self.cursor.execute(SQL, arguments)
        result = self.cursor.fetchall()
        return result
