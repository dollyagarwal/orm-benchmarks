# Test B→ Bulk Insert


import os
import time
import pandas as pd
from warehouses.models import *

import sys
from django.db import connection

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "mysite.settings")

try:
    import django  # noqa

    django.setup()  # noqa
finally:
    pass

mock_data = 'MOCK_DATA.csv'


def main():
    print("test D done at: ", time.ctime())
    count = 0

    citys = ('Moscow', 'St. Petersbrg', 'Pshkin', 'Oraneinbaum', 'Vladivostok')

    table = []
    rows = []
    times = []
    rowspersec = []

    start = time.time()
    for _ in range(10):
        print(f'-------------------ITERATION {_} ---------------------------')
        for city in citys:
            start_city = time.time()
            res = list(warehouse.objects.filter(city=city).all())
            now_city = time.time()
            print(warehouse.objects.filter(city=city).all().query)
            print('City:', city.encode("utf-8"), '#Records:', len(res), 'Time taken: ', now_city - start_city)
            count += len(res)
    now = time.time()
    print(connection.queries)
    print(f"Django Testcase D, Rows fetched: {count}, time taken: {(now - start)}")
    print(f"Django Testcase D, Filter on warehouse: Rows/sec: {count / (now - start): 10.2f}")

    time_taken = now - start
    table.append('Warehouse')
    rows.append(count)
    times.append(time_taken)
    rowspersec.append((count / time_taken))

    print("Table with relation: District (filter on city)")
    citys = ('Moscow', 'St. Petersbrg', 'Pshkin', 'Oraneinbaum', 'Vladivostok')  # data['city'].tolist()  #
    start = time.time()
    for _ in range(10):
        print(f'-------------------ITERATION {_} ---------------------------')
        for city in citys:
            start_city = time.time()
            res = list(district.objects.filter(city=city).all())
            now_city = time.time()
            print(warehouse.objects.filter(city=city).all().query)
            print('City:', city.encode("utf-8"), '#Records:', len(res), 'Time taken: ', now_city - start_city)
            count += len(res)
    now = time.time()
    print(connection.queries)
    print(f"Django Testcase D, Rows fetched: {count}, time taken: {(now - start)}")
    print(f"Django Testcase D, Filter on District: Rows/sec: {count / (now - start): 10.2f}")
    time_taken = now - start
    table.append('District')
    rows.append(count)
    times.append(time_taken)
    rowspersec.append((count / time_taken))

    print("Big Table with relation: Stock (filter on quantity)")

    start = time.time()
    for _ in range(10):
        print(f'-------------------ITERATION {_} ---------------------------')
        for qty in range(1, 11):
            start_city = time.time()
            res = list(stock.objects.filter(quantity=qty).all())
            now_city = time.time()
            print(warehouse.objects.filter(city=city).all().query)
            print('City:', city.encode("utf-8"), '#Records:', len(res), 'Time taken: ', now_city - start_city)
            count += len(res)
    now = time.time()
    print(connection.queries)
    print(f"Django Testcase D, Rows fetched: {count}, time taken: {(now - start)}")
    print(f"Django Testcase D, Filter on Stock: Rows/sec: {count / (now - start): 10.2f}")

    time_taken = now - start
    table.append('Stock')
    rows.append(count)
    times.append(time_taken)
    rowspersec.append((count / time_taken))

    output = pd.DataFrame({'Table': table, 'Rows': rows, 'Total Time': times, 'RowsPerSec': rowspersec})
    output['ORM'] = 'Django'
    output['Testcase'] = 'D'
    output.to_csv('outputs/test_d.csv')


if __name__ == '__main__':
    file_path = 'outputs/test_d.txt'
    sys.stdout = open(file_path, "w+")

    main()
