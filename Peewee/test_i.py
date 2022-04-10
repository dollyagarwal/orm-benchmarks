import time
from random import randint

import pandas as pd

from models import *

testcase = 'I'
orm = 'Peewee'

QUANTITY = list(range(1, 10))

mock_data = 'MOCK_DATA.csv'

db = PostgresqlExtDatabase("project", user="postgres", password="password", host="localhost", port=5432)

objs = list(Item.select())
count = len(objs)


def main():
    print("--------------------Running Test I-------------------")
    result = pd.DataFrame()
    id_count = 5001

    start = time.time()

    with db.atomic():
        for obj in objs:
            obj.id = id_count
            obj.name = f"{obj.name}1"
            obj.price = randint(1, 100000)
            obj.data = f"{obj.data}1"
            obj.save()
            id_count += 1

    now = time.time()
    query_res = {'Testcase': testcase, 'Table': 'Item', 'Rows': count, 'Total time': f"{(now - start):10.2f}",
                 'RowsPerSec': f"{count / (now - start): 10.2f}", "ORM": orm}
    result = result.append(query_res, ignore_index=True)
    print(f"Peewee Testcase I, Rows fetched: {count}, time taken: {now - start}")
    print(f"Peewee Testcase I, Rows/sec: {count / (now - start): 10.2f}")
    result.to_csv('peewee_outputs/test_i.csv', encoding='utf-8')


if __name__ == '__main__':
    main()
