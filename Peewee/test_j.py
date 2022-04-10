import time

import pandas as pd

from models import *

testcase = 'J'
orm = 'Peewee'

QUANTITY = list(range(1, 10))

mock_data = 'MOCK_DATA.csv'

db = PostgresqlExtDatabase("project", user="postgres", password="password", host="localhost", port=5432)

objs = list(Item.select())
count = len(objs)


def main():
    print("--------------------Running Test J-------------------")
    result = pd.DataFrame()
    start = time.time()

    for obj in objs:
        obj.name = f"{obj.name} Update1"
        obj.save(only=["name"])

    now = time.time()
    query_res = {'Testcase': testcase, 'Table': 'Item', 'Rows': count, 'Total time': f"{(now - start):10.2f}",
                 'RowsPerSec': f"{count / (now - start): 10.2f}", "ORM": orm}
    result = result.append(query_res, ignore_index=True)
    print(f"Peewee Testcase J, Rows fetched: {count}, time taken: {now - start}")
    print(f"Peewee Testcase J, Rows/sec: {count / (now - start): 10.2f}")
    result.to_csv('peewee_outputs/test_j.csv', encoding='utf-8')


if __name__ == '__main__':
    main()
