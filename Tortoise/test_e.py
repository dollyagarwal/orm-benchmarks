import asyncio
import sys
import time

import pandas as pd
from tortoise import run_async

from models import *
from setup import create_db

file_path = 'outputs/test_e.txt'
sys.stdout = open(file_path, "a")

mock_data = 'MOCK_DATA.csv'
concurrents = 10

i = 0


async def _runtest_warehouse(iters, citys) -> int:
    count = 0

    global i
    for _ in range(iters // 10):
        for city in citys:
            offset = 100
            res = list(await warehouse.filter(city=city).limit(20).offset(offset))
            count += len(res)
            i += 1

    return count


async def _runtest_district(iters, citys) -> int:
    count = 0

    global i
    for _ in range(iters):
        for city in citys:
            offset = 1000
            res = list(await district.filter(city=city).limit(20).offset(offset))
            count += len(res)
            i += 1

    return count


async def _runtest_stock(iters) -> int:
    count = 0

    global i
    for _ in range(0, iters, 90000):
        for qty in range(1, 11):
            offset = 10000
            res = list(await stock.filter(quantity=qty).limit(20).offset(offset))
            count += len(res)
            i += 1

    return count


async def run_benchmarks():
    await create_db()
    global i
    i = 0
    citys = ('Moscow', 'St. Petersbrg', 'Pshkin', 'Oraneinbaum', 'Vladivostok')

    table = []
    rows = []
    times = []
    rowspersec = []

    start = now = time.time()

    iters = 500
    count = sum(await asyncio.gather(*[_runtest_warehouse(iters // concurrents, citys)
                                       for _ in range(concurrents)]))

    now = time.time()

    table.append('Warehouse')
    rows.append(count)
    times.append(now - start)
    rowspersec.append((count / (now - start)))
    print(f"Tortoise Testcase E, Rows fetched: {count}, time taken: {(now - start)}")
    print(f"Tortoise Testcase E, Filter on warehouse (citys): Rows/sec: {count / (now - start): 10.2f}")

    output = pd.DataFrame({'Table': table, 'Rows': rows, 'TotalTime': times, 'RowsPerSec': rowspersec})
    output['ORM'] = 'Tortoise'
    output['Testcase'] = 'E'
    output.to_csv('outputs/test_e.csv')
    print('i ', i)


run_async(run_benchmarks())
