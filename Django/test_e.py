# Test B→ Bulk Insert


import os
import time
import pandas as pd
from warehouses.models import *
from random import randrange
import sys

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "mysite.settings")

try:
    import django  # noqa

    django.setup()  # noqa
finally:
    pass


mock_data = 'MOCK_DATA.csv'


def main():
    print("test E done at: ", time.ctime())
    count = 0

    citys = ('Moscow', 'St. Petersbrg', 'Pshkin', 'Oraneinbaum', 'Vladivostok')
    start = time.time()

    table = []
    rows = []
    times = []
    rowspersec = []

    ITERATIONS = 500
    for _ in range(ITERATIONS // 10):
        # print(f'-------------------ITERATION {_} ---------------------------')
        for city in citys:
            offset = randrange(ITERATIONS - 20)
            res = list(warehouse.objects.filter(city=city).all()[offset: 20 + offset])
            count += len(res)
    now = time.time()
    print(f"Django Testcase E, Rows fetched: {count}, time taken: {(now - start)}")
    print(f"Django Testcase E, Warehouse: Rows/sec: {count / (now - start): 10.2f}")

    time_taken = now - start
    table.append('Warehouse')
    rows.append(count)
    times.append(time_taken)
    rowspersec.append((count / time_taken))

    output = pd.DataFrame({'Table': table, 'Rows': rows, 'Total Time': times, 'RowsPerSec': rowspersec})
    output['ORM'] = 'Django'
    output['Testcase'] = 'E'
    output.to_csv('outputs/test_e.csv')


if __name__ == '__main__':
    file_path = 'outputs/test_e.txt'
    sys.stdout = open(file_path, "w+")

    main()
