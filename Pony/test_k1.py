import time

import pandas as pd
from pony.orm import commit, db_session, select

from create_schema import Customer

file_path = 'output/PonyORM/test_k1.csv'
ORM = "PonyORM"
testcase = "K1"
final = pd.DataFrame()

with db_session():
    obj = list(select(c for c in Customer))
    count = len(obj)

    start = time.time()
    for object in obj:
        object.delete()
        commit()

    now = time.time()

query_time = {'Testcase': testcase, 'Table': 'Customer', 'Rows': count, 'Total time': f"{(now - start)}",
              'RowsPerSec': f"{count / (now - start)}", 'ORM': ORM}
final = final.append(query_time, ignore_index=True)
final.to_csv(file_path, encoding='utf-8')
