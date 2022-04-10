import time

import pandas as pd

from models import *

QUANTITY = list(range(1, 10))

testcase = 'E'
orm = 'Peewee'

mock_data = 'MOCK_DATA.csv'

db = PostgresqlExtDatabase("project", user="postgres", password="password", host="localhost", port=5432)


def filtercase():
    print("--------------------Running Test E-------------------")
    result = pd.DataFrame()
    citys = ('Moscow', 'St. Petersbrg', 'Pshkin', 'Oraneinbaum', 'Vladivostok')

    start = time.time()
    count = 0

    iters = 500

    for _ in range(iters // 10):
        for city in citys:
            offset = 100
            res = list(Warehouse.select().where(Warehouse.city == city).limit(20).offset(offset))
            count += len(res)

    now = time.time()
    query_res = {'Testcase': testcase, 'Table': 'Warehouse', 'Rows': count, 'Total time': f"{(now - start):10.2f}",
                 'RowsPerSec': f"{count / (now - start): 10.2f}", "ORM": orm}
    result = result.append(query_res, ignore_index=True)
    print(f"Peewee Testcase E, Rows fetched: {count}, time taken:{now - start}")
    print(f"Peewee Testcase E, Filter on warehouse: Rows/sec {count / (now - start): 10.2f}")

    result.to_csv('peewee_outputs/test_e.csv', encoding='utf-8')


def main():
    start = now = time.time()
    filtercase()
    now = time.time()
    print(f"Test E: total runtime: {(now - start): 10.2f}")


if __name__ == '__main__':
    main()
