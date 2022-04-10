import time
from datetime import datetime
from random import choice, randint, choices

import pandas as pd

from models import *
from settings import AMOUNT_OF_WAREHOUSES

testcase = 'C'
orm = 'Peewee'

mock_data = 'MOCK_DATA.csv'

db = PostgresqlExtDatabase("project", user="postgres", password="password", host="localhost", port=5432)


def populate(n):
    print("--------------------Running Test C-------------------")
    result = pd.DataFrame()
    data = pd.read_csv(mock_data, engine='python')
    citys = ('Moscow', 'St. Petersbrg', 'Pshkin', 'Oraneinbaum', 'Vladivostok')  # data['city'].tolist()  #
    citys = choices(citys, k=1000)
    names = data[
        'names'].tolist()
    last_names = data[
        'last_name'].tolist()

    start = now = time.time()
    nrows = 0

    Warehouse.insert_many(
        [(i, i, 'w_st %d' % i, 'w_st2 %d' % i, citys[i], 'w_zip %d' % i, float(i), 0) for i in range(1, n + 1)],
        [Warehouse.id,
         Warehouse.number,
         Warehouse.street_1,
         Warehouse.street_2,
         Warehouse.city,
         Warehouse.w_zip,
         Warehouse.tax,
         Warehouse.ytd]
    ).execute()
    nrows = n

    now = time.time()
    query_res = {'Testcase': testcase, 'Table': 'Warehouse', 'Rows': nrows, 'Total time': f"{(now - start):10.2f}",
                 'RowsPerSec': f"{nrows / (now - start): 10.2f}", 'ORM': orm}
    result = result.append(query_res, ignore_index=True)
    print(f"Peewee Testcase C, Warehouse: Rows: {nrows}, Time: {now - start}")
    print(f"Peewee Testcase C, Warehouse: Rows/sec: {nrows / (now - start): 10.2f}")

    start = now = time.time()
    d_cnt = 0
    nrows = 0

    District.insert_many(
        [(10 * (i - 1) + j + 1, i, 'dist %d %d' % (i, j), 'd_st %d' % j, 'd_st2 %d' % j, citys[i], 'd_zip %d' % j
          , float(j), 0) for i in range(1, n + 1) for j in range(10)],
        [District.id,
         District.warehouse_id,
         District.name,
         District.street_1,
         District.street_2,
         District.city,
         District.d_zip,
         District.tax,
         District.ytd]
    ).execute()

    nrows = 10 * n

    now = time.time()
    query_res = {'Testcase': testcase, 'Table': 'District', 'Rows': nrows, 'Total time': f"{(now - start):10.2f}",
                 'RowsPerSec': f"{nrows / (now - start): 10.2f}", "ORM": orm}
    result = result.append(query_res, ignore_index=True)
    print(f"Peewee Testcase C, District: Rows: {nrows}, Time: {now - start}")
    print(f"Peewee Testcase C, District: Rows/sec: {nrows / (now - start): 10.2f}")

    start = now = time.time()
    nrows = 0

    Customer.insert_many(
        [(i, choice(names), choice(names), choice(last_names), 'c_st %d' % i, 'c_st2 %d' % i,
          choice(citys), 'c_zip %d' % i, 'phone', datetime(2005, 7, 14, 12, 30), 'credit',
          randint(1000, 100000), choice((0, 10, 15, 20, 30)), 0, 0, 1000000, 0, 'customer %d' % i, 'hello %d' % i,
          randint(1, 10 * n)) for i in range(10 * n)],
        [Customer.id,
         Customer.first_name,
         Customer.middle_name,
         Customer.last_name,
         Customer.street_1,
         Customer.street_2,
         Customer.city,
         Customer.c_zip,
         Customer.phone,
         Customer.since,
         Customer.credit,
         Customer.credit_lim,
         Customer.discount,
         Customer.delivery_cnt,
         Customer.payment_cnt,
         Customer.balance,
         Customer.ytd_payment,
         Customer.data_1,
         Customer.data_2,
         Customer.district_id
         ]
    ).execute()
    now = time.time()

    nrows = 10 * n
    query_res = {'Testcase': testcase, 'Table': 'Customer', 'Rows': nrows, 'Total time': f"{(now - start):10.2f}",
                 'RowsPerSec': f"{nrows / (now - start): 10.2f}", 'ORM': orm}
    result = result.append(query_res, ignore_index=True)
    print(f"Peewee Testcase C, Customer: Rows: {nrows}, Time: {now - start}")
    print(f"Peewee Testcase C, Customer: Rows/sec: {nrows / (now - start): 10.2f}")

    start = now = time.time()
    nrows = 0

    Item.insert_many(
        [(i, 'item %d' % i, randint(1, 100000), 'data') for i in range(1, n * 10 + 1)],
        [Item.id, Item.name, Item.price, Item.data]
    ).execute()
    nrows = n * 10

    now = time.time()
    query_res = {'Testcase': testcase, 'Table': 'Item', 'Rows': nrows, 'Total time': f"{(now - start):10.2f}",
                 'RowsPerSec': f"{nrows / (now - start): 10.2f}", "ORM": orm}
    result = result.append(query_res, ignore_index=True)
    print(f"Peewee Testcase C, Item: Rows: {nrows}, Time: {now - start}")
    print(f"Peewee Testcase C, Item: Rows/sec: {nrows / (now - start): 10.2f}")

    start = now = time.time()
    nrows = 0

    for i in range(1, n * 10 + 1):
        for j in range(1, n + 1):
            nrows += 1
            Stock.insert_many(
                [(nrows, j, i, randint(1, 10), randint(1, 100000), 0, 0, "data")],
                [Stock.id, Stock.warehouse, Stock.item, Stock.quantity, Stock.ytd, Stock.order_cnt, Stock.remote_cnt,
                 Stock.data]
            ).execute()

    now = time.time()
    query_res = {'Testcase': testcase, 'Table': 'Stock', 'Rows': nrows, 'Total time': f"{(now - start):10.2f}",
                 'RowsPerSec': f"{nrows / (now - start): 10.2f}", "ORM": orm}
    result = result.append(query_res, ignore_index=True)
    print(f"Peewee Testcase C, Stock: Rows: {nrows}, Time: {now - start}")
    print(f"Peewee Testcase C, Stock: Rows/sec: {nrows / (now - start): 10.2f}")
    print(result)
    result.to_csv('peewee_outputs/test_c.csv', encoding='utf-8')


def main():
    create_tables()
    start = now = time.time()
    populate(AMOUNT_OF_WAREHOUSES)
    now = time.time()
    print(f"Test C: total runtime: {(now - start): 10.2f}")


if __name__ == '__main__':
    main()
