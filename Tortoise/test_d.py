import asyncio
import sys
import time

import pandas as pd
from tortoise import run_async

from models import *
from setup import create_db

file_path = 'outputs/test_d.txt'
sys.stdout = open(file_path, "a")

mock_data = 'MOCK_DATA.csv'
concurrents = 10


async def _runtest_warehouse(inrange, citys) -> int:
    count = 0

    for _ in range(inrange):
        for city in citys:
            res = list(await warehouse.filter(city=city).all())
            count += len(res)

    return count


async def _runtest_district(inrange, citys) -> int:
    count = 0

    for _ in range(inrange):
        for city in citys:
            res = list(await district.filter(city=city).all())
            count += len(res)

    return count


async def _runtest_stock(inrange) -> int:
    count = 0

    for _ in range(inrange):
        for qty in range(1, 11):
            res = list(await stock.filter(quantity=qty).all())
            count += len(res)

    return count


async def run_benchmarks():
    await create_db()
    data = pd.read_csv(mock_data)

    citys = ('Moscow', 'St. Petersbrg', 'Pshkin', 'Oraneinbaum', 'Vladivostok')

    inrange = 10 // concurrents
    if inrange < 1:
        inrange = 1

    table = []
    rows = []
    times = []
    rowspersec = []

    start = now = time.time()

    count = sum(await asyncio.gather(*[_runtest_warehouse(inrange, citys) for _ in range(concurrents)]))

    now = time.time()

    table.append('Warehouse')
    rows.append(count)
    times.append(now - start)
    rowspersec.append((count / (now - start)))
    print(f"Tortoise Testcase D, Rows fetched: {count}, time taken: {(now - start)}")
    print(f"Tortoise Testcase D, Filter on warehouse (citys): Rows/sec: {count / (now - start): 10.2f}")

    start = now = time.time()

    count = sum(await asyncio.gather(*[_runtest_district(inrange, citys) for _ in range(concurrents)]))

    now = time.time()

    table.append('District')
    rows.append(count)
    times.append(now - start)
    rowspersec.append((count / (now - start)))
    print(f"Tortoise Testcase D, Rows fetched: {count}, time taken: {(now - start)}")
    print(f"Tortoise Testcase D, Filter on district (citys): Rows/sec: {count / (now - start): 10.2f}")

    start = now = time.time()

    count = sum(await asyncio.gather(*[_runtest_stock(inrange) for _ in range(concurrents)]))

    now = time.time()

    table.append('Stock')
    rows.append(count)
    times.append(now - start)
    rowspersec.append((count / (now - start)))
    print(f"Tortoise Testcase D, Rows fetched: {count}, time taken: {(now - start)}")
    print(f"Tortoise Testcase D, Filter on stock (quantity): Rows/sec: {count / (now - start): 10.2f}")

    output = pd.DataFrame({'Table': table, 'Rows': rows, 'TotalTime': times, 'RowsPerSec': rowspersec})
    output['ORM'] = 'Tortoise'
    output['Testcase'] = 'D'
    output.to_csv('outputs/test_d.csv')


run_async(run_benchmarks())
