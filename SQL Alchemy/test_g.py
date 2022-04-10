import time

import pandas as pd
from sqlalchemy.orm import sessionmaker

from models import *
from settings import PROVIDER

testcase = 'G'
orm = 'SQLAlchemy'

engine = create_engine(PROVIDER['postgres'], echo=False)
Session = sessionmaker(bind=engine)
session = Session()


def main():
    result = pd.DataFrame()
    citys = ('Moscow', 'St. Petersbrg', 'Pshkin', 'Oraneinbaum', 'Vladivostok')

    count = 0
    start = time.time()
    for _ in range(10):
        for city in citys:
            res = [
                {k: v for k, v in value.__dict__.items() if k[:4] != "_sa_"}
                for value in session.query(Warehouse).filter(Warehouse.city == city).all()
            ]
            count += len(res)

    now = time.time()

    print(f"SQLAlchemy Testcase G, Rows fetched: {count}, time taken: {(now - start)}")
    print(f"Django Testcase G, Rows/sec: {count / (now - start): 10.2f}")
    query_res = {'Testcase': testcase, 'Table': 'Warehouse', 'Rows': count, 'Total time': f"{(now - start):10.2f}",
                 'RowsPerSec': f"{count / (now - start): 10.2f}", "ORM": orm}
    result = result.append(query_res, ignore_index=True)
    result.to_csv('sqlalchemy_outputs/test_g.csv')


if __name__ == '__main__':
    main()
