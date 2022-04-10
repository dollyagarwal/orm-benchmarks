import time

import pandas as pd

from models import *

testcase = 'D'
orm = 'Peewee'

QUANTITY = list(range(1, 11))

mock_data = 'MOCK_DATA.csv'

db = PostgresqlExtDatabase("project", user="postgres", password="password", host="localhost", port=5432)


def filtercase(iterations):
    print("--------------------Running Test D-------------------")
    result = pd.DataFrame()
    citys = ('Moscow', 'St. Petersbrg', 'Pshkin', 'Oraneinbaum', 'Vladivostok')
    start = time.time()

    count = 0

    for _ in range(iterations):
        for city in citys:
            res = list(Warehouse.select().where(Warehouse.city == city))
            count += len(res)

    now = time.time()
    query_res = {'Testcase': testcase, 'Table': 'Warehouse', 'Rows': count, 'Total time': f"{(now - start):10.2f}",
                 'RowsPerSec': f"{count / (now - start): 10.2f}", "ORM": orm}
    result = result.append(query_res, ignore_index=True)
    print(f"Peewee Testcase D, Rows fetched: {count}, time taken:{now - start}")
    print(f"Peewee Testcase D, Filter on warehouse: Rows/sec {count / (now - start): 10.2f}")

    start = time.time()
    count = 0

    for _ in range(iterations):
        for city in citys:
            res = list(District.select().where(District.city == city))
            count += len(res)

    now = time.time()
    query_res = {'Testcase': testcase, 'Table': 'District', 'Rows': count, 'Total time': f"{(now - start):10.2f}",
                 'RowsPerSec': f"{count / (now - start): 10.2f}", "ORM": orm}
    result = result.append(query_res, ignore_index=True)
    print(f"Peewee Testcase D, Rows fetched: {count}, time taken:{now - start}")
    print(f"Peewee Testcase D, Filter on district: Rows/sec {count / (now - start): 10.2f}")

    start = time.time()
    count = 0

    for _ in range(iterations):
        for qty in QUANTITY:
            res = list(Stock.select().where(Stock.quantity == qty))
            count += len(res)

    now = time.time()
    query_res = {'Testcase': testcase, 'Table': 'Stock', 'Rows': count, 'Total time': f"{(now - start):10.2f}",
                 'RowsPerSec': f"{count / (now - start): 10.2f}", "ORM": orm}
    result = result.append(query_res, ignore_index=True)
    print(f"Peewee Testcase D, Rows fetched: {count}, time taken:{now - start}")
    print(f"Peewee Testcase D, Filter on stock: Rows/sec {count / (now - start): 10.2f}")
    result.to_csv('peewee_outputs/test_d.csv', encoding='utf-8')


def main():
    start = now = time.time()
    filtercase(10)
    now = time.time()
    print(f"Test D: total runtime: {(now - start): 10.2f}")


if __name__ == '__main__':
    main()
