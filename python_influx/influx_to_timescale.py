import TimescaleDB
import Coinbase

influxclient = Coinbase.CoinbaseInfluxDBClient()
timescale = TimescaleDB.TimescaleDB()

influx_query = 'SELECT * FROM "BTC-EUR" WHERE trade_id > {min} AND trade_id < {max}'

ts_min = timescale.min_id()
influx_min = influxclient.get_min_trade_id("BTC-EUR")

while ts_min > influx_min:
    print("Min ID in TimescaleDB: " + str(ts_min))
    influx_data = influxclient.get_points(influx_query, "BTC-EUR", min=ts_min - 5000, max=ts_min)
    trades = [Coinbase.Trade.from_dict(point) for point in influx_data]
    print("Inserting...")
    timescale.insert(trades)
    print("Done")
    ts_min = timescale.min_id()

print("All data transferred.")