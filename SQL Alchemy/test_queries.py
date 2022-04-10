import time

import pandas as pd
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func

from models import *
from settings import PROVIDER

engine = create_engine(PROVIDER['postgres'], echo=True)
Session = sessionmaker(bind=engine)
session = Session()

testcase = 'queries'
orm = 'SQLAlchemy'


def main():
    result = pd.DataFrame()

    start = time.time()

    subquery = session.query(func.max(Stock.quantity).label('max_a1')).scalar_subquery()
    query = (session.query(Stock.warehouse_id, Stock.item_id).filter(Stock.quantity == (subquery)))

    now = time.time()

    count = 0
    for row in query:
        count += 1

    print(now, start, (now - start))

    query_res = {'Testcase': testcase, 'Table': 'Query1', 'Rows': count, 'Total time': f"{(now - start)}",
                 'RowsPerSec': f"{count / (now - start): 10.2f}", "ORM": orm}
    result = result.append(query_res, ignore_index=True)

    start = time.time()

    query = (session.query(Stock.warehouse_id).group_by(Stock.warehouse_id).having(func.avg(Stock.quantity) >= 5))

    now = time.time()

    count = 0
    for row in query:
        count += 1

    query_res = {'Testcase': testcase, 'Table': 'Query2', 'Rows': count, 'Total time': f"{(now - start)}",
                 'RowsPerSec': f"{count / (now - start): 10.2f}", "ORM": orm}
    result = result.append(query_res, ignore_index=True)

    start = time.time()
    query = (
        session.query(Stock.item_id, Stock.warehouse_id).join(Warehouse, Stock.warehouse_id == Warehouse.id).filter(
            Warehouse.city == 'Moscow'))
    now = time.time()
    count = 0
    for row in query:
        count += 1
    print('count: ', count)
    query_res = {'Testcase': testcase, 'Table': 'Query3', 'Rows': count, 'Total time': f"{(now - start)}",
                 'RowsPerSec': f"{count / (now - start): 10.2f}", "ORM": orm}
    result = result.append(query_res, ignore_index=True)

    result.to_csv('sqlalchemy_outputs/test_queries.csv')


if __name__ == '__main__':
    main()
