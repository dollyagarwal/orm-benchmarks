# Test B→ Bulk Insert


import os
import time
import pandas as pd
from warehouses.models import *
from django.db import transaction

import sys

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "mysite.settings")

try:
    import django  # noqa

    django.setup()  # noqa
finally:
    pass

mock_data = 'MOCK_DATA.csv'


def main():
    print("test K done at: ", time.ctime())
    objs = list(customer.objects.all())
    count = len(objs)

    start = time.time()

    table = []
    rows = []
    times = []
    rowspersec = []

    with transaction.atomic():
        for obj in objs:
            obj.delete()

    now = time.time()
    print(f"Django Testcase K, Rows deleted: {count}, time taken: {(now - start)}")
    print(f"Django Testcase K, Rows/sec: {count / (now - start): 10.2f}")

    time_taken = now - start
    table.append('Customer')
    rows.append(count)
    times.append(time_taken)
    rowspersec.append((count / time_taken))
    output = pd.DataFrame({'Table': table, 'Rows': rows, 'Total Time': times, 'RowsPerSec': rowspersec})
    output['ORM'] = 'Django'
    output['Testcase'] = 'K'
    output.to_csv('outputs/test_k.csv')


if __name__ == '__main__':
    file_path = 'outputs/test_k.txt'
    sys.stdout = open(file_path, "w+")

    main()
