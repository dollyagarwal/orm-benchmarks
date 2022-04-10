import time

import pandas as pd
from pony.orm import db_session, select

from create_schema import Warehouse

file_path = 'output/PonyORM/test_h.csv'
ORM = "PonyORM"
testcase = "H"
final = pd.DataFrame()

cities = ['Moscow', 'St. Petersbrg', 'Pshkin', 'Oraneinbaum', 'Vladivostok']
count = 0

start = time.time()

with db_session():
    for _ in range(10):
        for city in cities:
            res = list(select(w for w in Warehouse if w.city == city))
            count += len(res)
now = time.time()

print(count)
query_time = {'Testcase': testcase, 'Table': 'Warehouse', 'Rows': count, 'Total time': f"{(now - start)}",
              'RowsPerSec': f"{count / (now - start)}", 'ORM': ORM}
final = final.append(query_time, ignore_index=True)
final.to_csv(file_path, encoding='utf-8')
