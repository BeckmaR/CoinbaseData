from Coinbase import CoinbaseRESTService, CoinbaseInfluxDBClient
import time
import os
from operator import itemgetter


influxdb_host = os.getenv("INFLUXDB_HOST", "influxdb")

client = CoinbaseInfluxDBClient(host=influxdb_host)
product_id = "BTC-EUR"

query_limit = 'SELECT * FROM "{product_id}" WHERE "trade_id" <= {max_id} AND "trade_id" >={min_id}'

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
        if not trade_id in loaded_ids:
            coinbase_data = rest.get_id(product_id, trade_id)
            influx_points = [client.from_match(match, product_id) for match in coinbase_data]
            for point in influx_points:
                # Only write actual missing ids!
                point_id = point["fields"]["trade_id"]
                if point_id in trade_ids and not point_id in loaded_ids:
                    client.write(point)
                    print("Write: " + str(point))
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


def work(max_id, min_id):
    trades = client.get_points(query_limit, measurement="BTC-EUR", product_id="BTC-EUR", min_id=min_id, max_id=max_id)
    dupe_len = remove_duplicates(trades)
    if dupe_len > 0:
        trades = client.get_points(query_limit, measurement="BTC-EUR", product_id="BTC-EUR", min_id=min_id, max_id=max_id)
    trade_ids = [t["trade_id"] for t in trades]
    trade_ids.sort()
    get_missing(trade_ids)
    return min(trade_ids)


def get_max_validated():
    query = """SELECT LAST("trade_id") FROM "BTC-EUR_validated" """
    rs = client.query(query)
    for t in rs.get_points():
        return t["last"]


def get_minutes(max_id):
    return client.get_points(
        'SELECT * FROM "BTC-EUR_min_max_count_1m" WHERE max >= {max_id}',
        measurement="BTC-EUR_min_max_count_1m",
        max_id=max_id
    )


def check_minutes_connected(minutes):
    if len(minutes) > 1:
        minutes = sorted(minutes, key=itemgetter('min'))
        last_max = minutes[0]["min"] - 1
        for row in minutes:
            if last_max + 1 != row["min"]:
                work(min_id=last_max, max_id=row["max"])
            last_max = row["max"]


while True:
    max_id = get_max_validated()
    minutes = get_minutes(max_id)
    if len(minutes) > 0:
        for row in minutes:
            if row["max"] - row["min"] + 1 != row["count"]:
                work(max_id=row["max"], min_id=row["min"])
            else:
                print("{0} to {1} complete!".format(row["min"], row["max"]))
        check_minutes_connected(minutes)
        max_id = max([row["max"] for row in minutes])
        data = {
            "measurement": "BTC-EUR_validated",
            "fields": {"trade_id": max_id}
        }
        client.write(data)
    time.sleep(15)





