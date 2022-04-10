import sys
import time

import pandas as pd
import psycopg2

con = psycopg2.connect(database='project', user='postgres',
                       password='password')

file_path = 'outputs/test_queries.txt'
sys.stdout = open(file_path, "w+")

CITY_CHOICE = ['Moscow', 'St. Petersbrg', 'Pshkin', 'Oraneinbaum', 'Vladivostok']

QUANTITY = list(range(1, 11))

with con:
    cur = con.cursor()
    print("-----------Test Queries-------------")
    table = []
    rows = []
    times = []
    rowspersec = []

    start = time.time()
    cur.execute(f"SELECT s1.id FROM stock s1 WHERE s1.quantity = ALL ( SELECT MAX(s2.quantity) FROM stock s2);")
    count = cur.rowcount
    now = time.time()
    print(f"Testcase Queries1 , Rows fetched: {count}, time taken:{now - start}")
    print(f"Testcase Queries1, RowsperSec: Rows/sec {count / (now - start): 10.2f}")
    time_taken = now - start
    table.append('Query1')
    rows.append(count)
    times.append(time_taken)
    rowspersec.append((count / time_taken))

    start = time.time()

    cur.execute(f"SELECT s.warehouse_id FROM stock s GROUP BY s.warehouse_id HAVING AVG(s.quantity) >= 5;")
    count = cur.rowcount
    now = time.time()
    print(f"Testcase Queries2 , Rows fetched: {count}, time taken:{now - start}")
    print(f"Testcase Queries2, RowsperSec: Rows/sec {count / (now - start): 10.2f}")
    time_taken = now - start
    table.append('Query2')
    rows.append(count)
    times.append(time_taken)
    rowspersec.append((count / time_taken))

    start = time.time()
    cur.execute(f"SELECT s.id FROM stock s INNER JOIN warehouse w on s.warehouse_id = w.id WHERE w.city = 'Moscow';")
    count = cur.rowcount
    now = time.time()
    print(f"Testcase Queries3 , Rows fetched: {count}, time taken:{now - start}")
    print(f"Testcase Queries3, RowsperSec: Rows/sec {count / (now - start): 10.2f}")
    time_taken = now - start
    table.append('Query3')
    rows.append(count)
    times.append(time_taken)
    rowspersec.append((count / time_taken))

    output = pd.DataFrame({'Table': table, 'Rows': rows, 'Total Time': times, 'RowsPerSec': rowspersec})
    output['ORM'] = 'Native'
    output['Testcase'] = 'queries'
    output.to_csv('outputs/test_queries.csv')
