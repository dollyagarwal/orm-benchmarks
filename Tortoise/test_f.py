import asyncio
import time
import sys
from models import *
from tortoise import run_async
from setup import create_db
from settings import AMOUNT_OF_WAREHOUSES
from random import randint
import pandas as pd

file_path = 'outputs/test_f.txt'
sys.stdout = open(file_path, "a")

mock_data = 'MOCK_DATA.csv'
concurrents = 10


async def _runtest_warehouse(count, maxval) -> int:

    for _ in range(count):
        await warehouse.get(id=randint(1, maxval))

    return count


async def _runtest_district(count, maxval) -> int:

    for _ in range(count):
        await district.get(id=randint(1, maxval))

    return count


async def _runtest_stock(count, maxval) -> int:

    for _ in range(count):
        await stock.get(id=randint(1, maxval))

    return count


async def run_benchmarks():
    await create_db()

    count = AMOUNT_OF_WAREHOUSES
    maxval = count - 1
    count *= 2

    table = []
    rows = []
    times = []
    rowspersec = []

    start = now = time.time()

    count = sum(await asyncio.gather(*[_runtest_warehouse(count // concurrents, maxval) for _ in range(concurrents)]))

    now = time.time()

    table.append('Warehouse')
    rows.append(count)
    times.append(now - start)
    rowspersec.append((count / (now - start)))
    print(f"Tortoise Testcase F, Rows fetched: {count}, time taken: {(now - start)}")
    print(f"Tortoise Testcase F, Get on warehouse: Rows/sec: {count / (now - start): 10.2f}")

    start = now = time.time()

    count = sum(await asyncio.gather(*[_runtest_district(count // concurrents, maxval) for _ in range(concurrents)]))

    now = time.time()

    table.append('District')
    rows.append(count)
    times.append(now - start)
    rowspersec.append((count / (now - start)))
    print(f"Tortoise Testcase F, Rows fetched: {count}, time taken: {(now - start)}")
    print(f"Tortoise Testcase F, Get on district: Rows/sec: {count / (now - start): 10.2f}")

    start = now = time.time()

    count = sum(await asyncio.gather(*[_runtest_stock(count // concurrents, maxval)
                                       for _ in range(concurrents)]))

    now = time.time()

    table.append('Stock')
    rows.append(count)
    times.append(now - start)
    rowspersec.append((count / (now - start)))
    print(f"Tortoise Testcase F, Rows fetched: {count}, time taken: {(now - start)}")
    print(f"Tortoise Testcase F, Get on stock: Rows/sec: {count / (now - start): 10.2f}")

    output = pd.DataFrame({'Table': table, 'Rows': rows, 'TotalTime': times, 'RowsPerSec': rowspersec})
    output['ORM'] = 'Tortoise'
    output['Testcase'] = 'F'
    output.to_csv('outputs/test_f.csv')


run_async(run_benchmarks())
