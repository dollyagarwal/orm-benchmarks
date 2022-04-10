# Test B→ Bulk Insert


import os
import sys
import time

import pandas as pd
from django.db import transaction

from warehouses.models import *

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "mysite.settings")

try:
    import django  # noqa

    django.setup()  # noqa
finally:
    pass

mock_data = 'MOCK_DATA.csv'


def main():
    print("test J done at: ", time.ctime())

    objs = list(item.objects.all())
    count = len(objs)

    start = time.time()

    table = []
    rows = []
    times = []
    rowspersec = []

    with transaction.atomic():
        for obj in objs:
            data = 'Updated data'
            obj.save(update_fields=[data])

    now = time.time()
    print(f"Django Testcase J, Rows updated: {count}, time taken: {(now - start)}")
    print(f"Django Testcase J, Rows/sec: {count / (now - start): 10.2f}")

    time_taken = now - start
    table.append('Item')
    rows.append(count)
    times.append(time_taken)
    rowspersec.append((count / time_taken))
    output = pd.DataFrame({'Table': table, 'Rows': rows, 'Total Time': times, 'RowsPerSec': rowspersec})
    output['ORM'] = 'Django'
    output['Testcase'] = 'J'
    output.to_csv('outputs/test_j.csv')


if __name__ == '__main__':
    file_path = 'outputs/test_j.txt'
    sys.stdout = open(file_path, "w+")

    main()
