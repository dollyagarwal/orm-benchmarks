import time

import pandas as pd
from sqlalchemy.orm import sessionmaker

from models import *
from settings import PROVIDER

CITY_CHOICE = ['Moscow', 'St. Petersbrg', 'Pshkin', 'Oraneinbaum', 'Vladivostok']
QUANTITY = list(range(1, 10))

engine = create_engine(PROVIDER['postgres'], echo=False)
Session = sessionmaker(bind=engine)

testcase = 'E'
orm = 'SQLAlchemy'


def main():
    result = pd.DataFrame()
    # print("--------------------Running Test E-------------------")
    start = time.time()

    session = Session()
    count = 0

    iters = 500

    for _ in range(iters // 10):
        for city in CITY_CHOICE:
            offset = 100
            res = list(session.query(Warehouse).filter(Warehouse.city == city).limit(20).offset(offset))
            count += len(res)
            print(len(res))
    now = time.time()

    query_res = {'Testcase': testcase, 'Table': 'Warehouse', 'Rows': count, 'Total time': f"{(now - start):10.2f}",
                 'RowsPerSec': f"{count / (now - start): 10.2f}", "ORM": orm}
    result = result.append(query_res, ignore_index=True)
    print(f"SQLAlchemy Testcase E, Rows fetched: {count}, time taken:{now - start}")
    print(f"SQLAlchemy Testcase E, Filter on warehouse: Rows/sec {count / (now - start): 10.2f}")

    result.to_csv('sqlalchemy_outputs/test_e.csv', encoding='utf-8')


if __name__ == '__main__':
    main()
