import asyncio
import time
import sys
from models import *
from tortoise import run_async
from setup import create_db
from tortoise.transactions import in_transaction
import pandas as pd

file_path = 'outputs/test_k.txt'
sys.stdout = open(file_path, "a")

concurrents = 10


async def _runtest(objs) -> int:
    async with in_transaction():
        for obj in objs:
            await obj.delete()

    return len(objs)


async def run_benchmarks():
    await create_db()

    objs = list(await customer.all())
    inrange = len(objs) // concurrents
    if inrange < 1:
        inrange = 1

    table = []
    rows = []
    times = []
    rowspersec = []

    start = now = time.time()

    count = sum(
        await asyncio.gather(
            *[_runtest(objs[i * inrange: ((i + 1) * inrange) - 1]) for i in range(concurrents)]
        )
    )

    now = time.time()

    table.append('Customer')
    rows.append(count)
    times.append(now - start)
    rowspersec.append((count / (now - start)))
    print(f"Tortoise Testcase K, Rows deleted: {count}, time taken: {(now - start)}")
    print(f"Tortoise Testcase K: Rows/sec: {count / (now - start): 10.2f}")

    output = pd.DataFrame({'Table': table, 'Rows': rows, 'TotalTime': times, 'RowsPerSec': rowspersec})
    output['ORM'] = 'Tortoise'
    output['Testcase'] = 'K'
    output.to_csv('outputs/test_k.csv')


run_async(run_benchmarks())
