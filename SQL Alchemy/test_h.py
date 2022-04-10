import time

import pandas as pd
from sqlalchemy.orm import sessionmaker

from models import *

Session = sessionmaker(bind=engine)
session = Session()

testcase = 'H'
orm = 'SQLAlchemy'


def main():
    result = pd.DataFrame()
    citys = ('Moscow', 'St. Petersbrg', 'Pshkin', 'Oraneinbaum', 'Vladivostok')

    count = 0
    start = time.time()
    for _ in range(10):
        for city in citys:
            res = list(
                session.query(Warehouse)
                    .filter(Warehouse.city == city)
                    .with_entities(*Warehouse.__table__._columns)
            )
            count += len(res)

    now = time.time()

    query_res = {'Testcase': testcase, 'Table': 'Warehouse', 'Rows': count, 'Total time': f"{(now - start):10.2f}",
                 'RowsPerSec': f"{count / (now - start): 10.2f}", "ORM": orm}
    result = result.append(query_res, ignore_index=True)
    result.to_csv('sqlalchemy_outputs/test_h.csv')


if __name__ == "__main__":
    main()
