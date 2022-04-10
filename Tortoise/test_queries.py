import time
from models import *
import sys
import time

import pandas as pd
from tortoise import run_async
from tortoise.functions import Avg, Max

from models import *
from setup import create_db

file_path = 'outputs/test_queries.txt'
sys.stdout = open(file_path, "a")


async def main():
    await create_db()
    print("---------Retrieve ID of all stocks with maximum quantity------")

    table = []
    rows = []
    times = []
    rowspersec = []
    start = time.time()

    max_quantity = await stock.annotate(max=Max('quantity')).values("max")
    res = await stock.annotate(max=Max("quantity")).filter(max=max_quantity[0]['max']).group_by('warehouse_id',
                                                                                                'item_id').values(
        'warehouse_id', 'item_id')

    now = time.time()
    count = len(res)
    time_taken = now - start
    table.append('Query1')
    rows.append(count)
    times.append(time_taken)
    rowspersec.append((count / time_taken))
    print(stock.annotate(max=Max('quantity')).values("max").sql())
    print(stock.annotate(max=Max("quantity")).filter(max=max_quantity[0]['max']).group_by('warehouse_id',
                                                                                          'item_id').values(
        'warehouse_id', 'item_id').sql())

    print(f"Tortoise Testcase query1, Rows fetched: {count}, time taken: {(now - start)}")
    print(f"Tortoise Testcase query1, Rows/sec: {count / (now - start): 10.2f}")

    print("---------Find warehouse id from stock table where average quantity >= 5------")
    start = time.time()
    res = await stock.annotate(avg=Avg("quantity")).filter(avg__gte=5).group_by("warehouse_id").values("warehouse_id")
    now = time.time()
    count = len(res)
    time_taken = now - start
    table.append('Query2')
    rows.append(count)
    times.append(time_taken)
    rowspersec.append((count / time_taken))
    print(stock.annotate(avg=Avg("quantity")).filter(avg__gte=5).group_by("warehouse_id").values("warehouse_id").sql())
    print(f"Tortoise Testcase query2, Rows fetched: {count}, time taken: {(now - start)}")
    print(f"Tortoise Testcase query2, Rows/sec: {count / (now - start): 10.2f}")

    print("---------Find stockid from all warehouses in Moscow------")
    start = time.time()
    res = await stock.all().select_related().filter(warehouse__city='Moscow').values('warehouse_id', 'item_id')
    now = time.time()
    count = len(res)
    time_taken = now - start
    table.append('Query3')
    rows.append(count)
    times.append(time_taken)
    rowspersec.append((count / time_taken))

    print(stock.all().select_related().filter(warehouse__city='Moscow').values('warehouse_id', 'item_id').sql())
    print(f"Tortoise Testcase query3, Rows fetched: {count}, time taken: {(now - start)}")
    print(f"Tortoise Testcase query3, Rows/sec: {count / (now - start): 10.2f}")

    output = pd.DataFrame({'Table': table, 'Rows': rows, 'TotalTime': times, 'RowsPerSec': rowspersec})
    output['ORM'] = 'Tortoise'
    output['Testcase'] = 'Queries'
    output.to_csv('outputs/TortoiseTestcaseQueries.csv')


run_async(main())
