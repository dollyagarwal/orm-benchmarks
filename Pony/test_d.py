import time

import pandas as pd
from pony.orm import db_session, select

from create_schema import Warehouse, District, Stock

file_path = 'output/PonyORM/test_d.csv'
cities = ['Moscow', 'St. Petersbrg', 'Pshkin', 'Oraneinbaum', 'Vladivostok']

ORM = "PonyORM"
testcase = "D"
final = pd.DataFrame()

start = time.time()
count = 0

with db_session():
    for _ in range(10):
        for city in cities:
            res = list(select(w for w in Warehouse if w.city == city))
            count += len(res)

now = time.time()

query_time = {'Testcase': testcase, 'Table': 'Warehouse', 'Rows': count, 'Total time': f"{(now - start)}",
              'RowsPerSec': f"{count / (now - start)}", 'ORM': ORM}
final = final.append(query_time, ignore_index=True)

start = time.time()
count = 0

with db_session():
    for _ in range(10):
        for city in cities:
            res = list(select(d for d in District if d.city == city))
            count += len(res)

now = time.time()

query_time = {'Testcase': testcase, 'Table': 'District', 'Rows': count, 'Total time': f"{(now - start)}",
              'RowsPerSec': f"{count / (now - start)}", 'ORM': ORM}
final = final.append(query_time, ignore_index=True)

start = time.time()
count = 0

quantity = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
with db_session:
    for _ in range(10):
        for qty in quantity:
            res = list(select(s for s in Stock if s.quantity == qty))
            count += len(res)

now = time.time()

query_time = {'Testcase': testcase, 'Table': 'Stock', 'Rows': count, 'Total time': f"{(now - start)}",
              'RowsPerSec': f"{count / (now - start)}", 'ORM': ORM}
final = final.append(query_time, ignore_index=True)

final.to_csv(file_path, encoding='utf-8')
