import sys
import time

import pandas as pd
import psycopg2

con = psycopg2.connect(database='project', user='postgres',
                       password='password')

file_path = 'outputs/native_test_k.txt'
sys.stdout = open(file_path, "w+")

CITY_CHOICE = ['Moscow', 'St. Petersbrg', 'Pshkin', 'Oraneinbaum', 'Vladivostok']

QUANTITY = list(range(1, 11))

with con:
    cur = con.cursor()
    print("-----------Test K-------------")
    table = []
    rows = []
    times = []
    rowspersec = []

    count = 0
    start = time.time()
    cur.execute(f"delete from customer")
    count = 5000

    now = time.time()

    print(f"Testcase I Item, Rows updated: {count}, time taken:{now - start}")
    print(f"Testcase I, Update on Item: Rows/sec {count / (now - start): 10.2f}")
    time_taken = now - start
    table.append('Customer')
    rows.append(count)
    times.append(time_taken)
    rowspersec.append((count / time_taken))

    output = pd.DataFrame({'Table': table, 'Rows': rows, 'Total Time': times, 'RowsPerSec': rowspersec})
    output['ORM'] = 'Native'
    output['Testcase'] = 'K'
    output.to_csv('outputs/test_k.csv')
