import asyncio
import time
import sys
from models import *
from tortoise import run_async
from setup import create_db
from random import randint
from tortoise.transactions import in_transaction
import pandas as pd

file_path = 'outputs/test_i.txt'
sys.stdout = open(file_path, "a")

mock_data = 'MOCK_DATA.csv'
concurrents = 10


async def _runtest(objs) -> int:
    async with in_transaction():
        i = 0
        for obj in objs:
            obj.name = 'new_item %d' % i
            obj.price = randint(1, 100000)
            obj.data = 'Updated data'
            await obj.save()
            i += 1

    return len(objs)


async def run_benchmarks():
    await create_db()

    objs = list(await item.all())
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

    table.append('Item')
    rows.append(count)
    times.append(now - start)
    rowspersec.append((count / (now - start)))
    print(f"Tortoise Testcase I, Rows updated: {count}, time taken: {(now - start)}")
    print(f"Tortoise Testcase I, Tortoise update all rows of Item, I: Rows/sec: {count / (now - start): 10.2f}")

    output = pd.DataFrame({'Table': table, 'Rows': rows, 'TotalTime': times, 'RowsPerSec': rowspersec})
    output['ORM'] = 'Tortoise'
    output['Testcase'] = 'I'
    output.to_csv('outputs/test_i.csv')


run_async(run_benchmarks())
