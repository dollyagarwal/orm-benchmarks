import time

import pandas as pd

from models import *
from settings import AMOUNT_OF_WAREHOUSES
from test_b import populate

testcase = 'K'
orm = 'Peewee'

QUANTITY = list(range(1, 10))

mock_data = 'MOCK_DATA.csv'

db = PostgresqlExtDatabase("project", user="postgres", password="password", host="localhost", port=5432)

objs = list(Customer.select())
count = len(objs)


def main():
    print("--------------------Running Test K-------------------")
    result = pd.DataFrame()
    start = time.time()

    for obj in objs:
        obj.delete_instance()

    now = time.time()
    query_res = {'Testcase': testcase, 'Table': 'Item', 'Rows': count, 'Total time': f"{(now - start):10.2f}",
                 'RowsPerSec': f"{count / (now - start): 10.2f}", "ORM": orm}
    result = result.append(query_res, ignore_index=True)
    print(f"Peewee Testcase K, Rows deleted: {count}, time taken: {now-start}")
    print(f"Peewee Testcase K, Delete rows once a time: Rows/sec: {count / (now - start): 10.2f}")

    print("Repopulating")
    db.drop_tables([Warehouse, District, Order, Stock, Item, OrderLine, Customer, History], cascade=True)
    create_tables()
    populate(AMOUNT_OF_WAREHOUSES)

    print("--------------------Running Test K-------------------")

    start = time.time()

    with db.atomic():
        for obj in objs:
            obj.delete_instance()

    now = time.time()
    query_res = {'Testcase': testcase, 'Table': 'Item', 'Rows': count, 'Total time': f"{(now - start):10.2f}",
                 'RowsPerSec': f"{count / (now - start): 10.2f}", "ORM": orm}
    result = result.append(query_res, ignore_index=True)
    print(f"Peewee Testcase K, Rows deleted: {count}, time taken: {now-start}")
    print(f"Peewee Testcase K, Delete all rows at one time: Rows/sec: {count / (now - start): 10.2f}")
    result.to_csv('peewee_outputs/test_k.csv', encoding='utf-8')


if __name__ == '__main__':
    main()
