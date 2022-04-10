# Test B→ Bulk Insert


import os
import time
import pandas as pd
from warehouses.models import *

import sys

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "mysite.settings")

try:
    import django  # noqa

    django.setup()  # noqa
finally:
    pass

mock_data = 'MOCK_DATA.csv'


def main():
    print("Test H done at: ", time.ctime())
    citys = ('Moscow', 'St. Petersbrg', 'Pshkin', 'Oraneinbaum', 'Vladivostok')

    count = 0
    start = time.time()

    table = []
    rows = []
    times = []
    rowspersec = []

    for _ in range(10):
        for city in citys:
            res = list(warehouse.objects.filter(city=city).all().values_list())
            count += len(res)

    now = time.time()
    print(f"Django Testcase H, Rows fetched: {count}, time taken: {(now - start)}")
    print(f"Django Testcase H, Rows/sec: {count / (now - start): 10.2f}")

    time_taken = now - start
    table.append('Warehouse')
    rows.append(count)
    times.append(time_taken)
    rowspersec.append((count / time_taken))
    output = pd.DataFrame({'Table': table, 'Rows': rows, 'Total Time': times, 'RowsPerSec': rowspersec})
    output['ORM'] = 'Django'
    output['Testcase'] = 'H'
    output.to_csv('outputs/test_h.csv')


if __name__ == '__main__':
    file_path = 'outputs/test_h.txt'

    sys.stdout = open(file_path, "w")

    main()
