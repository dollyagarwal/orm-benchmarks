# Test B→ Bulk Insert


import os
import time
import pandas as pd
from warehouses.models import *
from django.db.models import Avg, Max

import sys

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "mysite.settings")

try:
    import django  # noqa

    django.setup()  # noqa
finally:
    pass


def main():
    print("---------Retrieve ID of all stocks with maximum quantity------")
    print(stock.objects.filter(quantity__gte=stock.objects.aggregate(Max('quantity'))['quantity__max']).values(
        'warehouse_id', 'item_id').query)

    table = []
    rows = []
    times = []
    rowspersec = []
    start = time.time()

    res = stock.objects.filter(quantity__gte=stock.objects.aggregate(Max('quantity'))['quantity__max']).values(
        'warehouse_id', 'item_id')
    now = time.time()
    count = len(res)
    time_taken = now - start
    table.append('Query1')
    rows.append(count)
    times.append(time_taken)
    rowspersec.append((count / time_taken))

    print(f"Django Testcase query1, Rows fetched: {count}, time taken: {(now - start)}")
    print(f"Django Testcase query1, Rows/sec: {count / (now - start): 10.2f}")

    print("---------Find warehouse id from stock table where average quantity >= 5------")
    print(stock.objects.values('warehouse_id').annotate(Avg('quantity')).filter(quantity__avg__gte=5).values(
        'warehouse_id').query)
    start = time.time()
    res = stock.objects.values('warehouse_id').annotate(Avg('quantity')).filter(quantity__avg__gte=5).values(
        'warehouse_id')
    now = time.time()
    count = len(res)
    time_taken = now - start
    table.append('Query2')
    rows.append(count)
    times.append(time_taken)
    rowspersec.append((count / time_taken))
    print(f"Django Testcase query2, Rows fetched: {count}, time taken: {(now - start)}")
    print(f"Django Testcase query2, Rows/sec: {count / (now - start): 10.2f}")

    print("---------Find stockid from all warehouses in Moscow------")
    print(stock.objects.select_related().filter(warehouse__city='Moscow').values('warehouse_id', 'item_id').query)
    start = time.time()
    res = stock.objects.select_related().filter(warehouse__city='Moscow').values('warehouse_id', 'item_id')
    now = time.time()
    count = len(res)
    time_taken = now - start
    table.append('Query3')
    rows.append(count)
    times.append(time_taken)
    rowspersec.append((count / time_taken))

    print(f"Django Testcase query3, Rows fetched: {count}, time taken: {(now - start)}")
    print(f"Django Testcase query3, Rows/sec: {count / (now - start): 10.2f}")

    output = pd.DataFrame({'Table': table, 'Rows': rows, 'TotalTime': times, 'RowsPerSec': rowspersec})
    output['ORM'] = 'Django'
    output['Testcase'] = 'Queries'
    output.to_csv('outputs/DjangoTestcaseQueries.csv')


if __name__ == '__main__':
    file_path = 'outputs/test_queries.txt'
    sys.stdout = open(file_path, "w+")

    main()
