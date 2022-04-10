import time

import pandas as pd
from pony.orm import db_session, select

from create_schema import Warehouse

file_path = 'output/PonyORM/test_e.csv'

cities = ['Moscow', 'St. Petersbrg', 'Pshkin', 'Oraneinbaum', 'Vladivostok']
ORM = "PonyORM"
testcase = "E"
final = pd.DataFrame()

start = time.time()
count = 0
iter = 500
with db_session():
    for _ in range(iter // 10):
        for city in cities:
            offset = 100
            res = list(select(w for w in Warehouse if w.city == city).limit(20, offset))
            count += len(res)

now = time.time()

query_time = {'Testcase': testcase, 'Table': 'Warehouse', 'Rows': count, 'Total time': f"{(now - start)}",
              'RowsPerSec': f"{count / (now - start)}", 'ORM': ORM}
final = final.append(query_time, ignore_index=True)

final.to_csv(file_path, encoding='utf-8')
