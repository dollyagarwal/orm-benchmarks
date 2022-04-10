import time
from random import randint

import pandas as pd
from pony.orm import db_session, select

from create_schema import Warehouse, District, Stock

file_path = 'output/PonyORM/test_f.csv'

ORM = "PonyORM"
testcase = "F"
final = pd.DataFrame()

count = 500
iter = 1000
start = time.time()

with db_session():
    for _ in range(iter):
        val = randint(1, count)
        select(w for w in Warehouse if w.id == val).get()

now = time.time()

query_time = {'Testcase': testcase, 'Table': 'Warehouse', 'Rows': iter, 'Total time': f"{(now - start)}",
              'RowsPerSec': f"{iter / (now - start)}", 'ORM': ORM}
final = final.append(query_time, ignore_index=True)

count = 5000
iter = 1000
start = time.time()

with db_session():
    for _ in range(iter):
        val = randint(1, count)
        select(d for d in District if d.id == val).get()

now = time.time()
query_time = {'Testcase': testcase, 'Table': 'District', 'Rows': iter, 'Total time': f"{(now - start)}",
              'RowsPerSec': f"{iter / (now - start)}", 'ORM': ORM}
final = final.append(query_time, ignore_index=True)

count = 2500000
iter = 1000
start = time.time()

with db_session():
    for _ in range(iter):
        val = randint(1, count)
        select(s for s in Stock if s.id == val).get()

now = time.time()
query_time = {'Testcase': testcase, 'Table': 'Stock', 'Rows': iter, 'Total time': f"{(now - start)}",
              'RowsPerSec': f"{iter / (now - start)}", 'ORM': ORM}
final = final.append(query_time, ignore_index=True)
final.to_csv(file_path, encoding='utf-8')
