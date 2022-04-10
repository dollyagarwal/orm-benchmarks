# Test B→ Bulk Insert


import os
import sys
import time
from random import randint

import pandas as pd

from settings import AMOUNT_OF_WAREHOUSES
from warehouses.models import *

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "mysite.settings")

try:
    import django  # noqa

    django.setup()  # noqa
finally:
    pass

mock_data = 'MOCK_DATA.csv'


def main():
    print("test F done at: ", time.ctime())
    table = []
    rows = []
    times = []
    rowspersec = []

    count = AMOUNT_OF_WAREHOUSES
    maxval = count - 1
    count *= 2
    start = time.time()

    for _ in range(count):
        warehouse.objects.get(id=randint(1, maxval))

    now = time.time()
    print(f"Django Testcase F, Rows fetched: {count}, time taken: {(now - start)}")
    print(f"Django Testcase F, Warehouse: Rows/sec: {count / (now - start): 10.2f}")

    time_taken = now - start
    table.append('Warehouse')
    rows.append(count)
    times.append(time_taken)
    rowspersec.append((count / time_taken))

    start = time.time()
    for _ in range(count):
        district.objects.get(id=randint(1, maxval))

    now = time.time()
    print(f"Django Testcase F, Rows fetched: {count}, time taken: {(now - start)}")
    print(f"Django Testcase F, District: Rows/sec: {count / (now - start): 10.2f}")

    time_taken = now - start
    table.append('District')
    rows.append(count)
    times.append(time_taken)
    rowspersec.append((count / time_taken))

    start = time.time()
    for _ in range(count):
        stock.objects.get(id=randint(1, maxval))
    now = time.time()

    print(f"Django Testcase F, Rows fetched: {count}, time taken: {(now - start)}")
    print(f"Django Testcase F, Stock: Rows/sec: {count / (now - start): 10.2f}")

    time_taken = now - start
    table.append('Stock')
    rows.append(count)
    times.append(time_taken)
    rowspersec.append((count / time_taken))

    output = pd.DataFrame({'Table': table, 'Rows': rows, 'Total Time': times, 'RowsPerSec': rowspersec})
    output['ORM'] = 'Django'
    output['Testcase'] = 'F'
    output.to_csv('outputs/test_f.csv')


if __name__ == '__main__':
    file_path = 'outputs/test_f.txt'
    sys.stdout = open(file_path, "w+")

    main()
