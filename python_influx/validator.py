from Coinbase import CoinbaseRESTService
from TimescaleDB import TimescaleDB
import time


tsdb = TimescaleDB()

rest = CoinbaseRESTService()


def missing(l, min, max):
    l.sort()
    result = list(set(l).symmetric_difference(range(min, max + 1)))
    result.sort()
    return result


def duplicates(trade_ids):
    seen = {}
    duplicates = []
    for trade_id in trade_ids:
        if trade_id not in seen:
            seen[trade_id] = 1
        else:
            duplicates.append(trade_id)
    return duplicates


def from_rest(trade_ids):
    trade_ids.sort(reverse=True)
    loaded_ids = []
    for trade_id in trade_ids:
        if not trade_id in loaded_ids:
            trades = rest.get_id("BTC-EUR", trade_id)
            for trade in trades:
                # Only write actual missing ids!
                trade_id = trade.trade_id
                if trade_id in trade_ids and not trade_id in loaded_ids:
                    tsdb.insert(trade)
                    print("Write: " + str(trade))
                    loaded_ids.append(trade_id)
    print("Written: " + str(loaded_ids))


def remove_duplicates(trades):
    dupes = duplicates(trades)
    if len(dupes) > 0:
        print("Duplicate: " + str(dupes))
        for d in dupes:
            tsdb.delete(d)
    return len(dupes)


def get_missing(trade_ids, min, max):
    missing_ids = missing(trade_ids, min, max)
    if len(missing_ids) > 0:
        print("Missing: " + str(missing_ids))
        from_rest(missing_ids)


def work(min_id, max_id):
    trades = tsdb.get_range(min_id, max_id)
    trade_ids = list(map(lambda i : i.trade_id, trades))
    dupe_len = remove_duplicates(trade_ids)
    if dupe_len > 0:
        trades = tsdb.get_range(min_id, max_id)
        trade_ids = list(map(lambda t: t.trade_id, trades))
    get_missing(list(trade_ids), min_id, max_id)


def check_interval(min, max):
    print("Checking: %d to %d" % (min, max))
    SQL = "SELECT count(trade_id), max(trade_id) - min(trade_id) + 1 from BTCEUR WHERE trade_id >= %s AND trade_id <= %s"
    result = tsdb.execute(SQL, (min, max))[0]
    if result[1] != result[0] or max - min + 1 != result[0]:
        if result[0] > 10000 or max - min > 10000:
            check_interval(min, int((max - min)/2 + min))
        else:
            work(min, max)
            check_interval(min, max)
    else:
        tsdb.set_max_validated(max)
        print("Complete: %d to %d" % (min, max))


while True:
    check_interval(tsdb.get_max_validated(), tsdb.max_id())
    time.sleep(15)





