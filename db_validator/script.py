from Coinbase import CoinbaseRESTService, CoinbaseInfluxDBClient
import time
import os


influxdb_host = os.getenv("INFLUXDB_HOST", "influxdb")

client = CoinbaseInfluxDBClient(host=influxdb_host)
product_id = "BTC-EUR"

query_limit = 'SELECT * FROM "{product_id}" WHERE "trade_id" < {trade_id} ORDER BY time DESC LIMIT 10100'.format(product_id=product_id, trade_id="{trade_id}")

rest = CoinbaseRESTService()


def missing(l):
    l.sort()
    result = list(set(l).symmetric_difference(range(l[0], l[-1] + 1)))
    result.sort()
    return result


def duplicates(l):
    seen = {}
    duplicates = []
    for n in l:
        trade_id = n["trade_id"]
        if trade_id not in seen:
            seen[trade_id] = 1
        else:
            duplicates.append(n)
    return duplicates


def delete(d):
    time = "'" + str(d["time"]) + "'"
    uid = "'" + str(d["trade_id"] % 100) + "'"
    query = 'DELETE FROM "BTC-EUR" WHERE time = {0} AND "uid" = {1}'.format(time, uid)
    print(query)
    client.query(query)


def from_rest(trade_ids):
    trade_ids.sort(reverse=True)
    loaded_ids = []
    for trade_id in trade_ids:
        coinbase_data = rest.get_id(product_id, trade_id)
        influx_points = [client.from_match(match, product_id) for match in coinbase_data]
        for point in influx_points:
            # Only write actual missing ids!
            point_id = point["fields"]["trade_id"]
            if point_id in trade_ids and not point_id in loaded_ids:
                client.write(point)
                loaded_ids.append(point_id)
    print("Written: " + str(loaded_ids))


def remove_duplicates(trades):
    dupes = duplicates(trades)
    if len(dupes) > 0:
        print("Duplicate: " + str(dupes))
        for d in dupes:
            delete(d)
    return len(dupes)


def get_missing(trade_ids):
    missing_ids = missing(trade_ids)
    if len(missing_ids) > 0:
        print("Missing: " + str(missing_ids))
        from_rest(missing_ids)


def work(max_trade_id):
    trades = query(max_trade_id)
    dupe_len = remove_duplicates(trades)
    if dupe_len > 0:
        trades = query(max_trade_id)
    trade_ids = [t["trade_id"] for t in trades]
    trade_ids.sort()
    trade_ids = trade_ids[-10000:]
    get_missing(trade_ids)
    return min(trade_ids)


def query(max_trade_id):
    query = query_limit.format(trade_id=max_trade_id)
    print(query)
    rs = client.query(query_limit.format(trade_id=max_trade_id))
    trades = [t for t in rs.get_points(product_id)]
    return trades


def min_id_in_trades(trades):
    return min([t["trade_id"] for t in trades])


def binary_search(min_id, max_id):
    print("Searching {0} to {1}...".format(min_id, max_id))
    query = \
        """SELECT min("trade_id"), max("trade_id"), count("trade_id")
        FROM "{product_id}" WHERE "trade_id" >= {min_id} AND "trade_id" <= {max_id}"""
    data = client.get_points(query, product_id, product_id=product_id, min_id=min_id, max_id=max_id)
    if len(data) != 1:
        print("Binary search returned unexpected results: " + str(data))
        return
    data = data[0]
    count = data["count"]
    min_id = data["min"]
    max_id = data["max"]
    if max_id - min_id == count - 1:
        print("{0} to {1} complete!".format(min_id, max_id))
        return
    if max_id - min_id > 10000:
        mid_id = int((min_id + max_id) / 2)
        binary_search(mid_id, max_id)
        binary_search(min_id, mid_id)
    else:
        work(max_id)


while True:
    binary_search(0, int(10e15))



