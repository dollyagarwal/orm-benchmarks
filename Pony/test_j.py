import time
from random import randint

import pandas as pd
from pony.orm import commit, db_session, select

from create_schema import Item

file_path = 'output/PonyORM/test_j.csv'
ORM = "PonyORM"
testcase = "J"
final = pd.DataFrame()

with db_session():
    obj = list(select(i for i in Item))
    count = len(obj)
    id = 1

    start = time.time()

    for object in obj:
        object.price = randint(1, 10000)

    commit()

    now = time.time()

query_time = {'Testcase': testcase, 'Table': 'Item', 'Rows': count, 'Total time': f"{(now - start)}",
              'RowsPerSec': f"{count / (now - start)}", 'ORM': ORM}
final = final.append(query_time, ignore_index=True)
final.to_csv(file_path, encoding='utf-8')
