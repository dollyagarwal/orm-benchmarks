import time

import pandas as pd
from pony.orm import db_session, select

from create_schema import Stock

file_path = 'output/PonyORM/test_Query2.csv'
ORM = "PonyORM"
testcase = "Query 2"
final = pd.DataFrame()

start = time.time()
with db_session():
    query_obj = select((s.warehouse_id, s.item_id) for s in Stock for w in s.warehouse_id if w.city == 'Moscow')
    sql = query_obj.get_sql()
    count = len(query_obj)
    print("The equivalent query is {}".format(sql))

now = time.time()
query_time = {'Testcase': testcase, 'Table': 'Stock', 'Rows': count, 'Total time': f"{(now - start)}",
              'RowsPerSec': f"{count / (now - start)}", 'ORM': ORM}
final = final.append(query_time, ignore_index=True)
final.to_csv(file_path, encoding='utf-8')
