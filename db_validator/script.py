from Coinbase import CoinbaseRESTService, CoinbaseInfluxDBClient
import time
import os


influxdb_host = os.getenv("INFLUXDB_HOST", "influxdb")

client = CoinbaseInfluxDBClient(host=influxdb_host)
product_id = "BTC-EUR"

query_limit = 'SELECT TOP("trade_id", 100000) AS "trade_id" from "{product_id}"'.format(product_id=product_id)
query_limit_where = query_limit + ' WHERE "trade_id" < {trade_id}'
query_min = 'SELECT min("trade_id") FROM "{product_id}"'.format(product_id=product_id)

rest = CoinbaseRESTService()


def missing(l):
    result = list(set(l).symmetric_difference(range(l[0], l[-1] + 1)))
    result.sort()
    return result


def from_rest(trade_ids):
    if len(trade_ids) == 0:
        return
    trade_id = max(trade_ids)
    coinbase_data = rest.get_id(product_id, trade_id)
    influx_points = [client.from_match(match, product_id) for match in coinbase_data]
    for point in influx_points:
        # Only write actual missing ids!
        if point["fields"]["trade_id"] in trade_ids:
            client.write(point)
        else:
            print("Discarding: " + str(point))


def validate_results(rs):
    tids = [int(t['trade_id']) for t in rs.get_points(product_id)]
    tids.sort()

    print(str(len(tids)) + " in DB")

    if tids[-1] - tids[0] == len(tids) - 1:
        print("No ids missing.")
    else:
        missing_ids = missing(tids)
        print("missing: " + str(len(missing_ids)) + ": " + str(missing_ids))
        if len(missing_ids) > 0:
            from_rest(missing_ids)
    return tids[0]


while True:
    if not client.has_ids_missing(product_id):
        time.sleep(20)
    else:
        rs = client.query(query_limit)

        min_id = validate_results(rs)
        while client.get_min_trade_id(product_id) != min_id:
            rs = client.query(query_limit_where.format(trade_id=min_id))

            min_id = validate_results(rs)
            time.sleep(5)
        time.sleep(20)



