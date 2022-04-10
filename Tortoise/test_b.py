import asyncio
import time
from models import *
from random import choice, randint
from datetime import datetime
from settings import AMOUNT_OF_WAREHOUSES
import pandas as pd
import sys
from tortoise import run_async
from setup import create_db
from tortoise.transactions import in_transaction

file_path = 'outputs/test_b.txt'
sys.stdout = open(file_path, "a")

mock_data = 'MOCK_DATA.csv'

w_c = 0  # to keep track of number of warehouses


async def _runtest_warehouse(count, citys):
    global w_c
    async with in_transaction():
        for i in range(count):
            w_c += 1
            await warehouse.create(
                number=w_c,
                street_1='w_st %d' % w_c,
                street_2='w_st2 %d' % w_c,
                city=citys[randint(0, 4)],
                w_zip='w_zip %d' % w_c,
                tax=float(w_c),
                ytd=0
            )


w_c_d = 0  # have to maintain a global variable of warehouses because of async operations
d_c = 0  # to keep track of number of districts


async def _runtest_district(count, citys):
    global w_c_d
    global d_c
    async with in_transaction():
        for i in range(count):
            w_c_d += 1
            for j in range(10):
                d_c += 1
                await district.create(
                    warehouse_id=w_c_d,
                    name='dist %d %d' % (w_c_d, j),
                    street_1='d_st %d' % j,
                    street_2='d_st2 %d' % j,
                    city=citys[randint(0, 4)],
                    d_zip='d_zip %d' % j,
                    tax=float(j),
                    ytd=0,
                )


c_c = 0  # to keep track of number of customers


async def _runtest_customer(count, names, last_names, citys):
    global d_c
    global c_c
    async with in_transaction():
        for i in range(count):
            c_c += 1
            await customer.create(
                first_name=choice(names),
                middle_name=choice(names),
                last_name=choice(last_names),
                street_1='c_st %d' % c_c,
                street_2='c_st2 %d' % c_c,
                city=choice(citys),
                c_zip='c_zip %d' % c_c,
                phone='phone',
                since=datetime(2005, 7, 14, 12, 30),
                credit='credit',
                credit_lim=randint(1000, 100000),
                discount=choice((0, 10, 15, 20, 30)),
                delivery_cnt=0,
                payment_cnt=0,
                balance=1000000,
                ytd_payment=0,
                data_1='customer %d' % c_c,
                data_2='hello %d' % c_c,
                district_id=randint(1, d_c),
            )


i_c = 0  # to keep track of number of items


async def _runtest_item(count):
    global i_c
    async with in_transaction():
        for i in range(count):
            i_c += 1
            await item.create(
                name='item %d' % i_c,
                price=randint(1, 100000),
                data='data'
            )


s_c = 0  # to keep track of number of stocks


async def _runtest_stock(lower_count, upper_count, j_count):
    global s_c
    async with in_transaction():
        for i in range(lower_count, upper_count):
            for j in range(1, j_count):
                s_c += 1
                await stock.create(
                    warehouse_id=j,
                    item_id=i,
                    quantity=randint(1, 10),
                    ytd=randint(1, 100000),
                    order_cnt=0,
                    remote_cnt=0,
                    data="data",
                )


async def populate(n):
    await create_db()
    data = pd.read_csv(mock_data)
    citys = ('Moscow', 'St. Petersbrg', 'Pshkin', 'Oraneinbaum', 'Vladivostok')
    names = data['names'].tolist()
    last_names = data['last_name'].tolist()
    concurrents = 10

    table = []
    rows = []
    times = []
    rowspersec = []

    start = time.time()
    count = n
    count = int(count // concurrents) * concurrents
    await asyncio.gather(*[_runtest_warehouse(count // concurrents, citys, ) for _ in range(concurrents)])

    now = time.time()

    table.append('Warehouse')
    rows.append(w_c)
    times.append(now - start)
    rowspersec.append((w_c / (now - start)))
    print(f"Tortoise Testcase B, warehouse: Rows: {w_c} , Time: {(now - start)}")
    print(f"Tortoise Testcase B, warehouse: Rows/sec: {w_c / (now - start): 10.2f}, timestamp: {datetime.now()}")

    start = time.time()

    count = n
    count = int(count // concurrents) * concurrents
    await asyncio.gather(*[_runtest_district(count // concurrents, citys, ) for _ in range(concurrents)])

    now = time.time()

    table.append('District')
    rows.append(d_c)
    times.append(now - start)
    rowspersec.append((d_c / (now - start)))
    print(f"Tortoise Testcase B, district: Rows: {d_c} , Time: {(now - start)}")
    print(f"Tortoise Testcase B, district: Rows/sec: {d_c / (now - start): 10.2f}, timestamp: {datetime.now()}")

    start = time.time()

    count = n * 10
    count = int(count // concurrents) * concurrents
    await asyncio.gather(
        *[_runtest_customer(count // concurrents, names, last_names, citys) for _ in range(concurrents)])

    now = time.time()

    table.append('Customer')
    rows.append(c_c)
    times.append(now - start)
    rowspersec.append((c_c / (now - start)))
    print(f"Tortoise Testcase B, customers: Rows: {c_c} , Time: {(now - start)}")
    print(f"Tortoise Testcase B, customers: Rows/sec: {c_c / (now - start): 10.2f}, timestamp: {datetime.now()}")

    start = time.time()

    count = (n * 10) + 1
    count = int(count // concurrents) * concurrents
    await asyncio.gather(*[_runtest_item(count // concurrents) for _ in range(concurrents)])

    now = time.time()

    table.append('Item')
    rows.append(i_c)
    times.append(now - start)
    rowspersec.append((i_c / (now - start)))
    print(f"Tortoise Testcase B, items: Rows: {i_c} , Time: {(now - start)}")
    print(f"Tortoise Testcase B, items: Rows/sec: {i_c / (now - start): 10.2f}, timestamp: {datetime.now()}")

    start = time.time()

    j_count = n + 1

    await asyncio.gather(
        _runtest_stock(1, n + 1, j_count),
        _runtest_stock(n + 1, 2 * n + 1, j_count),
        _runtest_stock(2 * n + 1, 3 * n + 1, j_count),
        _runtest_stock(3 * n + 1, 4 * n + 1, j_count),
        _runtest_stock(4 * n + 1, 5 * n + 1, j_count),
        _runtest_stock(5 * n + 1, 6 * n + 1, j_count),
        _runtest_stock(6 * n + 1, 7 * n + 1, j_count),
        _runtest_stock(7 * n + 1, 8 * n + 1, j_count),
        _runtest_stock(8 * n + 1, 9 * n + 1, j_count),
        _runtest_stock(9 * n + 1, 10 * n + 1, j_count)
    )

    now = time.time()

    table.append('Stock')
    rows.append(s_c)
    times.append(now - start)
    rowspersec.append((s_c / (now - start)))
    print(f"Tortoise Testcase B, stock: Rows: {s_c} , Time: {(now - start)}")
    print(f"Tortoise Testcase B, stock: Rows/sec: {s_c / (now - start): 10.2f}, timestamp: {datetime.now()}")

    output = pd.DataFrame({'Table': table, 'Rows': rows, 'TotalTime': times, 'RowsPerSec': rowspersec})
    output['ORM'] = 'Tortoise'
    output['Testcase'] = 'B'
    output.to_csv('outputs/test_b.csv')


async def run_benchmarks():
    await populate(AMOUNT_OF_WAREHOUSES)


run_async(run_benchmarks())
