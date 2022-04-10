import time

import pandas as pd

from models import *

mock_data = 'MOCK_DATA.csv'

testcase = 'G'
orm = 'Peewee'


def main():
    print("--------------------Running Test G-------------------")
    result = pd.DataFrame()
    citys = ('Moscow', 'St. Petersbrg', 'Pshkin', 'Oraneinbaum', 'Vladivostok')

    count = 0
    start = time.time()
    for _ in range(10):
        for city in citys:
            res = list(Warehouse.select().where(Warehouse.city == city).dicts())
            count += len(res)

    now = time.time()
    query_res = {'Testcase': testcase, 'Table': 'Warehouse', 'Rows': count, 'Total time': f"{(now - start):10.2f}",
                 'RowsPerSec': f"{count / (now - start): 10.2f}", "ORM": orm}
    result = result.append(query_res, ignore_index=True)
    print(f"Peewee Testcase G, Rows fetched: {count}, time taken:{now - start}")
    print(f"Peewee Testcase G, Rows/sec: {count / (now - start): 10.2f}")
    result.to_csv('peewee_outputs/test_g.csv', encoding='utf-8')


if __name__ == '__main__':
    main()
