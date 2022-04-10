import time

import pandas as pd
from sqlalchemy.orm import sessionmaker

from models import *
from settings import PROVIDER

CITY_CHOICE = ['Moscow', 'St. Petersbrg', 'Pshkin', 'Oraneinbaum', 'Vladivostok']

QUANTITY = list(range(1, 11))

engine = create_engine(PROVIDER['postgres'], echo=False)
Session = sessionmaker(bind=engine)

testcase = 'D'
orm = 'SQLAlchemy'


def main(iterations):
    result = pd.DataFrame()
    # print("--------------------Running Test D-------------------")
    start = time.time()

    session = Session()
    count = 0

    for _ in range(iterations):
        for city in CITY_CHOICE:
            res = list(session.query(Warehouse).filter(Warehouse.city == city))
            count += len(res)

    now = time.time()

    query_res = {'Testcase': testcase, 'Table': 'Warehouse', 'Rows': count, 'Total time': f"{(now - start):10.2f}",
                 'RowsPerSec': f"{count / (now - start): 10.2f}", "ORM": orm}
    result = result.append(query_res, ignore_index=True)
    print(f"SQLAlchemy Testcase D, Rows fetched: {count}, time taken:{now-start}")
    print(f"SQLAlchemy Testcase D, Filter on warehouse: Rows/sec {count / (now - start): 10.2f}")

    start = time.time()
    session = Session()
    count = 0

    for _ in range(iterations):
        for city in CITY_CHOICE:
            res = list(session.query(District).filter(District.city == city))
            count += len(res)

    now = time.time()

    query_res = {'Testcase': testcase, 'Table': 'District', 'Rows': count, 'Total time': f"{(now - start):10.2f}",
                 'RowsPerSec': f"{count / (now - start): 10.2f}", "ORM": orm}
    result = result.append(query_res, ignore_index=True)
    print(f"SQLAlchemy Testcase D, Rows fetched: {count}, time taken:{now-start}")
    print(f"SQLAlchemy Testcase D, Filter on district: Rows/sec {count / (now - start): 10.2f}")

    start = time.time()
    session = Session()
    count = 0

    for _ in range(iterations):
        for qty in QUANTITY:
            res = list(session.query(Stock).filter(Stock.quantity == qty))
            count += len(res)

    now = time.time()

    query_res = {'Testcase': testcase, 'Table': 'Stock', 'Rows': count, 'Total time': f"{(now - start):10.2f}",
                 'RowsPerSec': f"{count / (now - start): 10.2f}", "ORM": orm}
    result = result.append(query_res, ignore_index=True)
    print(f"SQLAlchemy Testcase D, Rows fetched: {count}, time taken:{now-start}")
    print(f"SQLAlchemy Testcase D, Filter on stock: Rows/sec {count / (now - start): 10.2f}")
    result.to_csv('sqlalchemy_outputs/test_d.csv', encoding='utf-8')


if __name__ == '__main__':
    main(10)
