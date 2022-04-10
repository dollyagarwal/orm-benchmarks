import time
from datetime import datetime
from random import choice, randint, choices

import pandas as pd

from models import *
from settings import AMOUNT_OF_WAREHOUSES

testcase = 'B'
orm = 'Peewee'

mock_data = 'MOCK_DATA.csv'

db = PostgresqlExtDatabase("project", user="postgres", password="password", host="localhost", port=5432)


def populate(n):
    print("--------------------Running Test B-------------------")
    result = pd.DataFrame()
    data = pd.read_csv(mock_data, engine='python')
    citys = ('Moscow', 'St. Petersbrg', 'Pshkin', 'Oraneinbaum', 'Vladivostok')
    citys = choices(citys, k=1000)
    names = data[
        'names'].tolist()
    last_names = data[
        'last_name'].tolist()

    start = now = time.time()
    nrows = 0
    with db.atomic():
        for i in range(1, n + 1):
            nrows += 1
            Warehouse.insert(
                id=i,
                number=i,
                street_1='w_st %d' % i,
                street_2='w_st2 %d' % i,
                city=citys[i],
                w_zip='w_zip %d' % i,
                tax=float(i),
                ytd=0
            ).execute()

    now = time.time()
    query_res = {'Testcase': testcase, 'Table': 'Warehouse', 'Rows': nrows, 'Total time': f"{(now - start):10.2f}",
                 'RowsPerSec': f"{nrows / (now - start): 10.2f}", 'ORM': orm}
    result = result.append(query_res, ignore_index=True)
    print(f"Peewee Testcase B, Warehouse: Rows: {nrows}, Time: {now - start}")
    print(f"Peewee Testcase B, Warehouse: Rows/sec: {nrows / (now - start): 10.2f}")

    start = now = time.time()
    d_cnt = 0
    nrows = 0
    with db.atomic():
        for i in range(1, n + 1):
            for j in range(10):
                nrows += 1
                District.insert(
                    id=nrows,
                    warehouse_id=i,
                    name='dist %d %d' % (i, j),
                    street_1='d_st %d' % j,
                    street_2='d_st2 %d' % j,
                    city=citys[i],
                    d_zip='d_zip %d' % j,
                    tax=float(j),
                    ytd=0,
                ).execute()
                d_cnt += 1
    now = time.time()
    query_res = {'Testcase': testcase, 'Table': 'District', 'Rows': nrows, 'Total time': f"{(now - start):10.2f}",
                 'RowsPerSec': f"{nrows / (now - start): 10.2f}", "ORM": orm}
    result = result.append(query_res, ignore_index=True)
    print(f"Peewee Testcase B, District: Rows: {nrows}, Time: {now - start}")
    print(f"Peewee Testcase B, District: Rows/sec: {nrows / (now - start): 10.2f}")

    start = now = time.time()
    nrows = 0
    with db.atomic():
        for i in range(10 * n):
            nrows += 1
            Customer.insert(
                id=i,
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
                data_1='customer %d' % i,
                data_2='hello %d' % i,
                district_id=randint(1, d_cnt),
            ).execute()
    now = time.time()
    query_res = {'Testcase': testcase, 'Table': 'Customer', 'Rows': nrows, 'Total time': f"{(now - start):10.2f}",
                 'RowsPerSec': f"{nrows / (now - start): 10.2f}", 'ORM': orm}
    result = result.append(query_res, ignore_index=True)
    print(f"Peewee Testcase B, Customer: Rows: {nrows}, Time: {now - start}")
    print(f"Peewee Testcase B, Customer: Rows/sec: {nrows / (now - start): 10.2f}")

    start = now = time.time()
    nrows = 0
    with db.atomic():
        for i in range(1, n * 10 + 1):
            nrows += 1
            Item.insert(
                id=i,
                name='item %d' % i,
                price=randint(1, 100000),
                data='data'
            ).execute()

    now = time.time()
    query_res = {'Testcase': testcase, 'Table': 'Item', 'Rows': nrows, 'Total time': f"{(now - start):10.2f}",
                 'RowsPerSec': f"{nrows / (now - start): 10.2f}", "ORM": orm}
    result = result.append(query_res, ignore_index=True)
    print(f"Peewee Testcase B, Item: Rows: {nrows}, Time: {now - start}")
    print(f"Peewee Testcase B, Item: Rows/sec: {nrows / (now - start): 10.2f}")

    start = now = time.time()
    nrows = 0
    with db.atomic():
        for i in range(1, n * 10 + 1):
            for j in range(1, n + 1):
                nrows += 1
                Stock.insert(
                    id=nrows,
                    warehouse_id=j,
                    item_id=i,
                    quantity=randint(1, 10),
                    ytd=randint(1, 100000),
                    order_cnt=0,
                    remote_cnt=0,
                    data="data",
                ).execute()

    now = time.time()
    query_res = {'Testcase': testcase, 'Table': 'Stock', 'Rows': nrows, 'Total time': f"{(now - start):10.2f}",
                 'RowsPerSec': f"{nrows / (now - start): 10.2f}", "ORM": orm}
    result = result.append(query_res, ignore_index=True)
    print(f"Peewee Testcase B, Stock: Rows: {nrows}, Time: {now - start}")
    print(f"Peewee Testcase B, Stock: Rows/sec: {nrows / (now - start): 10.2f}")
    print(result)
    result.to_csv('peewee_outputs/test_b.csv', encoding='utf-8')


def main():
    create_tables()
    start = now = time.time()
    populate(AMOUNT_OF_WAREHOUSES)
    now = time.time()
    print(f"Test B: total runtime: {(now - start): 10.2f}")


if __name__ == '__main__':
    main()
