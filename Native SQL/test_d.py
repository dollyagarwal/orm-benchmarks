import sys
import time

import pandas as pd
import psycopg2

con = psycopg2.connect(database='project', user='postgres',
                       password='password')

file_path = 'outputs/native_test_d.csv'
sys.stdout = open(file_path, "w+")

CITY_CHOICE = ['Moscow', 'St. Petersbrg', 'Pshkin', 'Oraneinbaum', 'Vladivostok']

QUANTITY = list(range(1, 11))

with con:
    cur = con.cursor()
    print("-----------Test D-------------")
    table = []
    rows_c = []
    times = []
    rowspersec = []

    count = 0
    start = time.time()
    for _ in range(10):
        for city in CITY_CHOICE:
            cur.execute(f"SELECT * FROM WAREHOUSE WHERE city='{city}'")
            rows = cur.fetchall()
            count += len(rows)

    now = time.time()

    print(f"Testcase D, Rows fetched: {count}, time taken:{now - start}")
    print(f"Testcase D, Filter on warehouse: Rows/sec {count / (now - start): 10.2f}")

    print('Length of lists are: ', len(table), len(rows_c), len(times), len(rowspersec))

    time_taken = now - start
    table.append('Warehouse')
    rows_c.append(count)
    times.append(time_taken)
    rowspersec.append((count / time_taken))

    start = time.time()
    count = 0

    for _ in range(10):
        for city in CITY_CHOICE:
            cur.execute(f"SELECT * FROM DISTRICT WHERE CITY='{city}'")
            rows = cur.fetchall()
            count += len(rows)

    now = time.time()

    time_taken = now - start
    table.append('District')
    rows_c.append(count)
    times.append(time_taken)
    rowspersec.append((count / time_taken))

    print(f"Testcase D District, Rows fetched: {count}, time taken:{now - start}")
    print(f"Testcase D, Filter on district: Rows/sec {count / (now - start): 10.2f}")

    print('Length of lists are: ', len(table), len(rows_c), len(times), len(rowspersec))

    start = time.time()
    count = 0

    for _ in range(10):
        for qty in QUANTITY:
            cur.execute(f"SELECT * FROM STOCK WHERE quantity={qty}")
            rows = cur.fetchall()
            count += len(rows)

    now = time.time()

    print(f"Testcase D Stock, Rows fetched: {count}, time taken:{now - start}")
    print(f"Testcase D, Filter on Stock: Rows/sec {count / (now - start): 10.2f}")

    print('Length of lists are: ', len(table), len(rows_c), len(times), len(rowspersec))

    time_taken = now - start
    table.append('Stock')
    rows_c.append(count)
    times.append(time_taken)
    rowspersec.append((count / time_taken))

    print('Length of lists are: ', len(table), len(rows_c), len(times), len(rowspersec))
    output = pd.DataFrame({'Table': table, 'Rows': rows_c, 'Total Time': times, 'RowsPerSec': rowspersec})
    output['ORM'] = 'Native'
    output['Testcase'] = 'D'
    output.to_csv('outputs/test_d.csv')
