import time
from datetime import datetime
from random import choice, randint

import pandas as pd
from pony.orm import commit, db_session

from create_schema import Warehouse, District, Customer, Item, Stock

file_path = 'output/PonyORM/test_b.csv'

data = pd.read_csv("MOCK_DATA.csv")

ORM = "PonyORM"
testcase = "B"
final = pd.DataFrame()

cities = ['Moscow', 'St. Petersbrg', 'Pshkin', 'Oraneinbaum', 'Vladivostok']
names = data['names '].tolist()
last_names = data.last_name.tolist()

start = now = time.time()
num_rows = 500

for i in range(1, 501):
    with db_session():
        Warehouse(number=i, street_1='w_st %d' % i, street_2='w_st2 %d' % i, city=choice(cities), w_zip='w_zip %d' % i,
                  tax=float(i), ytd=0)
    commit()

now = time.time()

query_time = {'Testcase': testcase, 'Table': 'Warehouse', 'Rows': num_rows, 'Total time': f"{(now - start)}",
              'RowsPerSec': f"{num_rows / (now - start)}", 'ORM': ORM}
final = final.append(query_time, ignore_index=True)

start = now = time.time()
d_cnt = 0
num_rows = 0

for i in range(1, 501):
    for j in range(10):
        num_rows = num_rows + 1
        with db_session():
            District(warehouse=i, name='dist %d %d' % (i, j), street_1='d_st %d' % j, street_2='d_st2 %d' % j,
                     city=choice(cities), d_zip='d_zip %d' % j, tax=float(j), ytd=0)
        commit()
        d_cnt = d_cnt + 1

now = time.time()

query_time = {'Testcase': testcase, 'Table': 'District', 'Rows': num_rows, 'Total time': f"{(now - start)}",
              'RowsPerSec': f"{num_rows / (now - start)}", 'ORM': ORM}
final = final.append(query_time, ignore_index=True)

start = now = time.time()
num_rows = 0

for i in range(10 * 500):
    num_rows += 1
    with db_session():
        Customer(first_name=choice(names), middle_name=choice(names), last_name=choice(last_names),
                 street_1='c_st %d' % i, street_2='c_st2 %d' % i, city=choice(cities), c_zip='c_zip %d' % i,
                 phone='phone', since=datetime(2005, 7, 14, 12, 30), credit='credit', credit_lim=randint(1000, 100000),
                 discount=choice((0, 10, 15, 20, 30)), delivery_cnt=0, payment_cnt=0, balance=1000000, ytd_payment=0,
                 data1='customer %d' % i, data2='hello %d' % i, district=randint(1, d_cnt))
    commit()

now = time.time()
query_time = {'Testcase': testcase, 'Table': 'Customer', 'Rows': num_rows, 'Total time': f"{(now - start)}",
              'RowsPerSec': f"{num_rows / (now - start)}", 'ORM': ORM}
final = final.append(query_time, ignore_index=True)

start = now = time.time()
num_rows = 0
for i in range(1, 500 * 10 + 1):
    num_rows += 1
    with db_session():
        Item(name='item %d' % i, price=randint(1, 100000), data='data')
    commit()

now = time.time()
query_time = {'Testcase': testcase, 'Table': 'Item', 'Rows': num_rows, 'Total time': f"{(now - start)}",
              'RowsPerSec': f"{num_rows / (now - start)}", 'ORM': ORM}
final = final.append(query_time, ignore_index=True)

start = now = time.time()
num_rows = 0

for i in range(1, 10 * 500 + 1):
    for j in range(1, 501):
        num_rows += 1
        with db_session():
            Stock(warehouse=j, item=i, quantity=randint(1, 10), ytd=randint(1, 100000), order_cnt=0, remote_cnt=0,
                  data="data")
        commit()

now = time.time()
query_time = {'Testcase': testcase, 'Table': 'Stock', 'Rows': num_rows, 'Total time': f"{(now - start)}",
              'RowsPerSec': f"{num_rows / (now - start)}", 'ORM': ORM}
final = final.append(query_time, ignore_index=True)

final.to_csv(file_path, encoding='utf-8')
