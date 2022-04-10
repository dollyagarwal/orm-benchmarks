import time
from random import randint

import pandas as pd

from models import *

testcase = 'F'
orm = 'Peewee'

QUANTITY = list(range(1, 10))

mock_data = 'MOCK_DATA.csv'

db = PostgresqlExtDatabase("project", user="postgres", password="password", host="localhost", port=5432)


def get():
    print("--------------------Running Test F-------------------")
    result = pd.DataFrame()
    count = 500
    maxval = count - 1
    count *= 2

    start = time.time()

    for _ in range(count):
        Warehouse.get(Warehouse.id == randint(1, maxval))

    now = time.time()
    query_res = {'Testcase': testcase, 'Table': 'Warehouse', 'Rows': count, 'Total time': f"{(now - start):10.2f}",
                 'RowsPerSec': f"{count / (now - start): 10.2f}", "ORM": orm}
    result = result.append(query_res, ignore_index=True)
    print(f"Peewee Testcase F, Rows fetched: {count}, time taken:{now - start}")
    print(f"Peewee Testcase F, Filter on warehouse: Rows/sec {count / (now - start): 10.2f}")

    for _ in range(count):
        District.get(District.id == randint(1, maxval))

    now = time.time()
    query_res = {'Testcase': testcase, 'Table': 'District', 'Rows': count, 'Total time': f"{(now - start):10.2f}",
                 'RowsPerSec': f"{count / (now - start): 10.2f}", "ORM": orm}
    result = result.append(query_res, ignore_index=True)
    print(f"Peewee Testcase F, Rows fetched: {count}, time taken:{now - start}")
    print(f"Peewee Testcase F, Filter on district: Rows/sec {count / (now - start): 10.2f}")

    start = time.time()

    for _ in range(count):
        Customer.get(Customer.id == randint(1, maxval))

    now = time.time()
    query_res = {'Testcase': testcase, 'Table': 'Stock', 'Rows': count, 'Total time': f"{(now - start):10.2f}",
                 'RowsPerSec': f"{count / (now - start): 10.2f}", "ORM": orm}
    result = result.append(query_res, ignore_index=True)
    print(f"Peewee Testcase F, Rows fetched: {count}, time taken:{now - start}")
    print(f"Peewee Testcase F, Filter on stock: Rows/sec {count / (now - start): 10.2f}")
    result.to_csv('peewee_outputs/test_f.csv', encoding='utf-8')


def main():
    start = now = time.time()
    get()
    now = time.time()
    print(f"Test E: total runtime: {(now - start): 10.2f}")


if __name__ == '__main__':
    main()
