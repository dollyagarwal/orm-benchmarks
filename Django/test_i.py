# Test B→ Bulk Insert


import os
import time
from random import randint
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


mock_data ='MOCK_DATA.csv'

def main():
    print("test I done at: ", time.ctime())

    count = 0
    start = time.time()

    objs = list(item.objects.all())
    count = len(objs)

    start = time.time()

    table = []
    rows = []
    times = []
    rowspersec = []

    i = 0
    with transaction.atomic():
        for obj in objs:
            obj.name = 'new_item %d' % i
            obj.price = randint(1, 100000)
            obj.data = 'Updated data'
            obj.save()
        i += 1

    now = time.time()

    print(f"Django Testcase I, Rows updated: {count}, time taken: {(now - start)}")
    print(f"Django Testcase I, Django update all rows of Item, I: Rows/sec: {count / (now - start): 10.2f}")

    time_taken = now - start
    table.append('Item')
    rows.append(count)
    times.append(time_taken)
    rowspersec.append((count / time_taken))
    output = pd.DataFrame({'Table': table, 'Rows': rows, 'Total Time': times, 'RowsPerSec': rowspersec})
    output['ORM'] = 'Django'
    output['Testcase'] = 'I'
    output.to_csv('outputs/test_i.csv')


if __name__ == '__main__':
    file_path = 'outputs/test_i.txt'
    sys.stdout = open(file_path, "w+")

    main()