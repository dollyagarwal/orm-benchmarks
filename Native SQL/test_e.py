import sys
import time
from random import randrange

import pandas as pd
import psycopg2

con = psycopg2.connect(database='project', user='postgres',
                       password='password')

file_path = 'outputs/native_test_e.csv'
sys.stdout = open(file_path, "w+")

CITY_CHOICE = ['Moscow', 'St. Petersbrg', 'Pshkin', 'Oraneinbaum', 'Vladivostok']

QUANTITY = list(range(1, 11))

with con:
    cur = con.cursor()
    print("-----------Test E-------------")
    table = []
    rows = []
    times = []
    rowspersec = []

    count = 0
    ITERATIONS = 500
    start = time.time()
    for _ in range(ITERATIONS // 10):
        for city in CITY_CHOICE:
            offset = randrange(ITERATIONS - 20)
            cur.execute(f"SELECT * FROM WAREHOUSE WHERE city='{city}' OFFSET {offset} LIMIT 20")
            rows_c = cur.fetchall()
            count = count + len(rows_c)

    now = time.time()

    print(f"Testcase E Warehouse, Rows fetched: {count}, time taken:{now - start}")
    print(f"Testcase E, Filter on Warehouse: Rows/sec {count / (now - start): 10.2f}")
    time_taken = now - start
    table.append('Warehouse')
    rows.append(count)
    times.append(time_taken)
    rowspersec.append((count / time_taken))

    output = pd.DataFrame({'Table': table, 'Rows': rows, 'Total Time': times, 'RowsPerSec': rowspersec})
    output['ORM'] = 'Native'
    output['Testcase'] = 'E'
    output.to_csv('outputs/test_e.csv')
