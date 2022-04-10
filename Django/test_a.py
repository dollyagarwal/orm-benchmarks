# Test A→ Single Insert

import os
import time
from random import choice, randint, choices
from datetime import datetime
import pandas as pd
from warehouses.models import *
from settings import AMOUNT_OF_WAREHOUSES
import sys

from django.db import connection

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "mysite.settings")

try:
    import django  # noqa

    django.setup()  # noqa
finally:
    pass

mock_data = 'MOCK_DATA.csv'


def populate(n):
    print("Test A done at: ", time.ctime())

    print("--------------------Running Test A-------------------")
    data = pd.read_csv(mock_data)

    citys = ('Moscow', 'St. Petersbrg', 'Pshkin', 'Oraneinbaum', 'Vladivostok')  # data['city'].tolist()  #
    citys = choices(citys, k=1000)
    names = data[
        'names'].tolist()
    last_names = data[
        'last_name'].tolist()

    start = now = time.time()
    nrows = 0

    table = []
    rows = []
    times = []
    rowspersec = []

    for i in range(1, n + 1):
        nrows += 1
        warehouse.objects.create(
            number=i,
            street_1='w_st %d' % i,
            street_2='w_st2 %d' % i,
            city=citys[i],
            w_zip='w_zip %d' % i,
            tax=float(i),
            ytd=0
        )
    now = time.time()
    time_taken = now - start
    table.append('Warehouse')
    rows.append(nrows)
    times.append(time_taken)
    rowspersec.append((nrows / time_taken))
    print(f"Django Testcase A, Warehouse: Rows: {nrows} , Time: {(now - start)}")
    print(f"Django Testcase A, Warehouse: Rows/sec: {nrows / (now - start): 10.2f}")

    start = now = time.time()
    d_cnt = 0
    nrows = 0

    for i in range(1, n + 1):
        for j in range(10):
            nrows += 1
            district.objects.create(
                warehouse_id=i,
                name='dist %d %d' % (i, j),
                street_1='d_st %d' % j,
                street_2='d_st2 %d' % j,
                city=citys[i],
                d_zip='d_zip %d' % j,
                tax=float(j),
                ytd=0,
            )
            d_cnt += 1
    now = time.time()
    print(f"Django Testcase A, District: Rows: {nrows} , Time: {(now - start)}")
    print(f"Django Testcase A, District: Rows/sec: {nrows / (now - start): 10.2f}")
    #
    time_taken = now - start
    table.append('District')
    rows.append(nrows)
    times.append(time_taken)
    rowspersec.append((nrows / time_taken))

    start = now = time.time()
    nrows = 0

    for i in range(10 * n):
        nrows += 1
        customer.objects.create(
            first_name=choice(names),
            middle_name=choice(names),
            last_name=choice(last_names),
            street_1='c_st %d' % i,
            street_2='c_st2 %d' % i,
            city=choice(citys),
            c_zip='c_zip %d' % i,
            phone='phone',
            since=datetime(2005, 7, 14, 12, 30),
            credit='credit',
            credit_lim=randint(1000, 100000),
            discount=choice((0, 10, 15, 20, 30)),
            delivery_cnt=0,
            payment_cnt=0,
            balance=1000000,
            ytd_payment=0,
            data1='customer %d' % i,
            data2='hello %d' % i,
            district_id=randint(1, d_cnt),
        )
    now = time.time()
    print(f"Django Testcase A, Customers: Rows: {nrows} , Time: {(now - start)}")
    print(f"Django Testcase A, Customers: Rows/sec: {nrows / (now - start): 10.2f}")
    time_taken = now - start
    table.append('Customer')
    rows.append(nrows)
    times.append(time_taken)
    rowspersec.append((nrows / time_taken))

    start = now = time.time()
    nrows = 0

    for i in range(1, n * 10 + 1):
        nrows += 1
        item.objects.create(
            name='item %d' % i,
            price=randint(1, 100000),
            data='data'
        )

    now = time.time()
    print(f"Django Testcase A, Items: Rows: {nrows} , Time: {(now - start)}")
    print(f"Django Testcase A, Items: Rows/sec: {nrows / (now - start): 10.2f}")
    time_taken = now - start
    table.append('Item')
    rows.append(nrows)
    times.append(time_taken)
    rowspersec.append((nrows / time_taken))

    start = now = time.time()
    nrows = 0

    for i in range(1, n * 10 + 1):
        for j in range(1, n + 1):
            nrows += 1
            stock.objects.create(
                warehouse_id=j,
                item_id=i,
                quantity=randint(1, 10),
                ytd=randint(1, 100000),
                order_cnt=0,
                remote_cnt=0,
                data="data",
            )

    now = time.time()
    print(f"Django Testcase A, Stock: Rows: {nrows} , Time: {(now - start)}")
    print(f"Django Testcase A, Stock: Rows/sec: {nrows / (now - start): 10.2f}")
    time_taken = now - start
    table.append('Stock')
    rows.append(nrows)
    times.append(time_taken)
    rowspersec.append((nrows / time_taken))
    output = pd.DataFrame({'Table': table, 'Rows': rows, 'Total Time': times, 'RowsPerSec': rowspersec})
    output['ORM'] = 'Django'
    output['Testcase'] = 'A'
    output.to_csv('outputs/test_a.csv')


if __name__ == '__main__':
    file_path = 'outputs/test_a.txt'
    sys.stdout = open(file_path, "w+")

    print("test done at: ", time.ctime())
    cursor = connection.cursor()
    cursor.execute('TRUNCATE TABLE public."warehouse" RESTART IDENTITY cascade')
    cursor.execute('TRUNCATE TABLE public."district" RESTART IDENTITY cascade')
    cursor.execute('TRUNCATE TABLE public."customer" RESTART IDENTITY cascade')
    cursor.execute('TRUNCATE TABLE public."order" RESTART IDENTITY cascade')
    cursor.execute('TRUNCATE TABLE public."item" RESTART IDENTITY cascade')

    start = now = time.time()
    populate(AMOUNT_OF_WAREHOUSES)
    now = time.time()
    print(f"Django Testcase A, total runtime: {(now - start): 10.2f}")
