import time
from random import randint

import pandas as pd
from sqlalchemy.orm import sessionmaker

from models import *
from settings import PROVIDER

CITY_CHOICE = ['Moscow', 'St. Petersbrg', 'Pshkin', 'Oraneinbaum', 'Vladivostok']

QUANTITY = list(range(1, 10))

engine = create_engine(PROVIDER['postgres'], echo=False)
Session = sessionmaker(bind=engine)

testcase = 'F'
orm = 'SQLAlchemy'


def main():
    # print("--------------------Running Test F-------------------")
    result = pd.DataFrame()
    start = time.time()

    session = Session()
    count = 500
    maxval = count - 1
    count *= 2

    for _ in range(count):
        session.query(Warehouse).get(randint(1, maxval))

    now = time.time()

    query_res = {'Testcase': testcase, 'Table': 'Warehouse', 'Rows': count, 'Total time': f"{(now - start):10.2f}",
                 'RowsPerSec': f"{count / (now - start): 10.2f}", "ORM": orm}
    result = result.append(query_res, ignore_index=True)
    print(f"SQLAlchemy Testcase F, Rows fetched: {count}, time taken:{now-start}")
    print(f"SQLAlchemy Testcase F, Filter on warehouse: Rows/sec {count / (now - start): 10.2f}")

    for _ in range(count):
        session.query(District).get(randint(1, maxval))

    now = time.time()

    query_res = {'Testcase': testcase, 'Table': 'District', 'Rows': count, 'Total time': f"{(now - start):10.2f}",
                 'RowsPerSec': f"{count / (now - start): 10.2f}", "ORM": orm}
    result = result.append(query_res, ignore_index=True)
    print(f"SQLAlchemy Testcase F, Rows fetched: {count}, time taken:{now-start}")
    print(f"SQLAlchemy Testcase F, Filter on district: Rows/sec {count / (now - start): 10.2f}")

    start = time.time()

    for _ in range(count):
        session.query(Customer).get(randint(1, maxval))

    now = time.time()

    query_res = {'Testcase': testcase, 'Table': 'Stock', 'Rows': count, 'Total time': f"{(now - start):10.2f}",
                 'RowsPerSec': f"{count / (now - start): 10.2f}", "ORM": orm}
    result = result.append(query_res, ignore_index=True)
    print(f"SQLAlchemy Testcase F, Rows fetched: {count}, time taken:{now-start}")
    print(f"SQLAlchemy Testcase F, Filter on stock: Rows/sec {count / (now - start): 10.2f}")
    result.to_csv('sqlalchemy_outputs/test_f.csv', encoding='utf-8')


if __name__ == '__main__':
    main()
