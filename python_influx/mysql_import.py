import influxdb
import pymysql

sql = pymysql.connect("jacomo","gdax", "gd4x", "gdax")
cursor = sql.cursor()
influx = influxdb.InfluxDBClient(host="jacomo", database="coinbase")


def get_min_id():
    rs = influx.query('select min("trade_id") from "BTC-EUR"')
    for r in rs.get_points("BTC-EUR"):
        return r["min"]


def run():
    min_id = get_min_id()
    print(min_id)
    query = "SELECT * FROM trades WHERE trade_id < {id} AND product_id = 1 ORDER BY trade_id DESC LIMIT 10000".format(id=min_id)
    print(query)
    cursor.execute(query)
    print("Done")
    result = cursor.fetchall()
    data = []
    for r in result:
        trade_id = r[0]
        if r[1] == 1:
            side = "sell"
        else:
            side = "buy"
        price_scale = r[2]
        size_scale = r[3]
        time = r[5]
        price_unscaled = r[6]
        size_unscaled = r[7]
        ms = int(time.timestamp() * 1000)

        point = {
            "measurement": "BTC-EUR",
            "tags": {
                "side": side,
                "uid": trade_id % 1000
            },
            "fields": {
                "size_unscaled": size_unscaled,
                "size_scale": -size_scale,
                "price_unscaled": price_unscaled,
                "price_scale": -price_scale,
                "trade_id": trade_id
            },
            "time": ms
        }
        data.append(point)
         #  print(point)
    influx.write_points(data, time_precision="ms")


while get_min_id() != 2955938:
    run()