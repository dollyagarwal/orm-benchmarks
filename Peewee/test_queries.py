import time

import pandas as pd
from peewee import JOIN
from peewee import fn

from models import *

testcase = 'queries'
orm = 'Peewee'


def main():
    result = pd.DataFrame()
    print("---------Retrieve ID of all stocks with maximum quantity------")
    print(Stock.select(Stock.warehouse_id, Stock.item_id).where(Stock.quantity == Stock.select(fn.Max(Stock.quantity))).sql())

    start = time.time()
    query = Stock.select(Stock.warehouse_id, Stock.item_id).where(Stock.quantity == Stock.select(fn.Max(Stock.quantity))).execute()
    now = time.time()
    rst = list(query)
    count = len(rst)
    print(f"Peewee Query 1, time taken:{now - start}")
    query_res = {'Testcase': testcase, 'Table': 'Query1', 'Rows': count, 'Total time': f"{(now - start):10.2f}",
                 'RowsPerSec': f"{count / (now - start): 10.2f}", "ORM": orm}
    result = result.append(query_res, ignore_index=True)

    print("---------Find warehouse id from stock table where average quantity >= 5------")
    print(Stock.select(Stock.warehouse).group_by(Stock.warehouse).having(fn.AVG(Stock.quantity) >= 5).sql())

    start = time.time()
    query = Stock.select(Stock.warehouse).group_by(Stock.warehouse).having(fn.AVG(Stock.quantity) >= 5).execute()
    now = time.time()
    rst = list(query)
    count = len(rst)
    print(f"Peewee Query 2, time taken:{now - start}")
    query_res = {'Testcase': testcase, 'Table': 'Query2', 'Rows': count, 'Total time': f"{(now - start):10.2f}",
                 'RowsPerSec': f"{count / (now - start): 10.2f}", "ORM": orm}
    result = result.append(query_res, ignore_index=True)

    print("---------Find stockid from all warehouses in Moscow------")
    print(Stock.select(Stock.warehouse_id, Stock.item_id).join(Warehouse, JOIN.INNER).where(Warehouse.city == 'Moscow').sql())

    start = time.time()
    query = Stock.select(Stock.warehouse_id, Stock.item_id).join(Warehouse, JOIN.INNER).where(Warehouse.city == 'Moscow').execute()
    now = time.time()
    rst = list(query)
    count = len(rst)
    print(f"Peewee Query 3, time taken:{now - start}")
    query_res = {'Testcase': testcase, 'Table': 'Query3', 'Rows': count, 'Total time': f"{(now - start):10.2f}",
                 'RowsPerSec': f"{count / (now - start): 10.2f}", "ORM": orm}
    result = result.append(query_res, ignore_index=True)
    result.to_csv('peewee_outputs/test_queries.csv', encoding='utf-8')


if __name__ == '__main__':
    main()
