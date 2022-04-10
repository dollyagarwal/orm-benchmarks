import asyncio
import time
import sys
from models import *
from tortoise import run_async
from setup import create_db
import pandas as pd

file_path = 'outputs/test_h.txt'
sys.stdout = open(file_path, "a")

mock_data = 'MOCK_DATA.csv'
concurrents = 10


async def _runtest(inrange, citys) -> int:
    count = 0

    for _ in range(inrange):
        for city in citys:
            res = list(await warehouse.filter(city=city).all().values_list())
            count += len(res)

    return count


async def run_benchmarks():
    await create_db()
    citys = ('Moscow', 'St. Petersbrg', 'Pshkin', 'Oraneinbaum', 'Vladivostok')

    inrange = 10 // concurrents
    if inrange < 1:
        inrange = 1

    table = []
    rows = []
    times = []
    rowspersec = []

    start = now = time.time()

    count = sum(await asyncio.gather(*[_runtest(inrange, citys) for _ in range(concurrents)]))

    now = time.time()

    table.append('Warehouse')
    rows.append(count)
    times.append(now - start)
    rowspersec.append((count / (now - start)))
    print(f"Tortoise Testcase G, Rows fetched: {count}, time taken: {(now - start)}")
    print(f"Tortoise Testcase G, Filter on warehouse: Rows/sec: {count / (now - start): 10.2f}")

    output = pd.DataFrame({'Table': table, 'Rows': rows, 'TotalTime': times, 'RowsPerSec': rowspersec})
    output['ORM'] = 'Tortoise'
    output['Testcase'] = 'H'
    output.to_csv('outputs/test_h.csv')


run_async(run_benchmarks())
