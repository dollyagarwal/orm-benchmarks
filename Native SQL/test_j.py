import sys
import time

import pandas as pd
import psycopg2

con = psycopg2.connect(database='project', user='postgres',
                       password='password')

file_path = 'outputs/native_test_j.txt'
sys.stdout = open(file_path, "w+")

CITY_CHOICE = ['Moscow', 'St. Petersbrg', 'Pshkin', 'Oraneinbaum', 'Vladivostok']

QUANTITY = list(range(1, 11))

with con:
    cur = con.cursor()
    print("-----------Test J-------------")
    table = []
    rows = []
    times = []
    rowspersec = []

    count = 0
    start = time.time()
    data = 'Updated data'
    cur.execute(f"UPDATE item set data='{data}'")
    count = 5000

    now = time.time()

    print(f"Testcase J Item, Rows updated: {count}, time taken:{now - start}")
    print(f"Testcase J, Update on Item: Rows/sec {count / (now - start): 10.2f}")
    time_taken = now - start
    table.append('Item')
    rows.append(count)
    times.append(time_taken)
    rowspersec.append((count / time_taken))

    output = pd.DataFrame({'Table': table, 'Rows': rows, 'Total Time': times, 'RowsPerSec': rowspersec})
    output['ORM'] = 'Native'
    output['Testcase'] = 'J'
    output.to_csv('outputs/test_j.csv')
