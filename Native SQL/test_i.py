import sys
import time
from random import randint

import pandas as pd
import psycopg2

con = psycopg2.connect(database='project', user='postgres',
                       password='password')

file_path = 'outputs/native_test_g.csv'
sys.stdout = open(file_path, "w+")

CITY_CHOICE = ['Moscow', 'St. Petersbrg', 'Pshkin', 'Oraneinbaum', 'Vladivostok']

QUANTITY = list(range(1, 11))

with con:
    cur = con.cursor()
    print("-----------Test I-------------")
    table = []
    rows = []
    times = []
    rowspersec = []

    count = 0
    start = time.time()
    for _ in range(5000):
        name = 'new_item %d' % _
        price = randint(1, 100000)
        data = 'Updated data'
        cur.execute(f"UPDATE item set price={price}, name='{name}', data='{data}' where id={_}")
        count += 1

    now = time.time()

    print(f"Testcase I Item, Rows updated: {count}, time taken:{now - start}")
    print(f"Testcase I, Update on Item: Rows/sec {count / (now - start): 10.2f}")
    time_taken = now - start
    table.append('Item')
    rows.append(count)
    times.append(time_taken)
    rowspersec.append((count / time_taken))

    output = pd.DataFrame({'Table': table, 'Rows': rows, 'Total Time': times, 'RowsPerSec': rowspersec})
    output['ORM'] = 'Native'
    output['Testcase'] = 'I'
    output.to_csv('outputs/test_i.csv')
