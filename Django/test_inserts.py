# Test A→ Single Insert

import os
import sys
import time

from django.db import connection

import test_a
import test_b
import test_c
from settings import AMOUNT_OF_WAREHOUSES

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "mysite.settings")

try:
    import django  # noqa

    django.setup()  # noqa
finally:
    pass

mock_data = 'MOCK_DATA.csv'


def start_truncate_insert(choice):
    cursor = connection.cursor()
    cursor.execute('TRUNCATE TABLE public."warehouse" RESTART IDENTITY cascade')
    cursor.execute('TRUNCATE TABLE public."district" RESTART IDENTITY cascade')
    cursor.execute('TRUNCATE TABLE public."customer" RESTART IDENTITY cascade')
    cursor.execute('TRUNCATE TABLE public."order" RESTART IDENTITY cascade')
    cursor.execute('TRUNCATE TABLE public."item" RESTART IDENTITY cascade')

    start = now = time.time()
    if choice == 0:
        test_a.populate(AMOUNT_OF_WAREHOUSES)
    elif choice == 1:
        test_b.populate(AMOUNT_OF_WAREHOUSES)
    elif choice == 2:
        test_c.populate(AMOUNT_OF_WAREHOUSES)
    now = time.time()
    print(f"Test {choice}: total insert runtime: {(now - start): 10.2f}")


def start_only_insert(choice):
    start = now = time.time()
    if choice == 0:
        test_a.populate(AMOUNT_OF_WAREHOUSES)
    elif choice == 1:
        test_b.populate(AMOUNT_OF_WAREHOUSES)
    elif choice == 2:
        test_c.populate(AMOUNT_OF_WAREHOUSES)
    now = time.time()
    print(f"Test {choice}: total insert runtime: {(now - start): 10.2f}")


def main():
    print("test done at: ", time.ctime())
    print("Starting Truncate-Insertion tests")
    for i in range(3):
        print("Starting truncate and insert: ", i)
        start = now = time.time()
        start_truncate_insert(i)
        now = time.time()
        print(f"Test {i}: total runtime: {(now - start): 10.2f}")
    print("All truncate and insert tests done successfully")

    print("Starting Only Insertion tests")

    for i in range(3):
        print("Starting insert: ", i)
        start = now = time.time()
        start_only_insert(i)
        now = time.time()
        print(f"Test {i}: total runtime: {(now - start): 10.2f}")
    print("All only insert tests done successfully")


if __name__ == '__main__':
    file_path = 'outputs/test_all_inserts.txt'
    sys.stdout = open(file_path, "w+")
    main()
