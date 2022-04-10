import time

import pandas as pd
from sqlalchemy.orm import sessionmaker

from models import *
from settings import PROVIDER

engine = create_engine(PROVIDER['postgres'], echo=False)
Session = sessionmaker(bind=engine)
session = Session()

testcase = 'K'
orm = 'SQLAlchemy'


def main():
    result = pd.DataFrame()
    objs = list(session.query(Customer).all())
    count = len(objs)

    start = time.time()

    for obj in objs:
        session.delete(obj)
    session.commit()

    now = time.time()

    query_res = {'Testcase': testcase, 'Table': 'Item', 'Rows': count, 'Total time': f"{(now - start):10.2f}",
                 'RowsPerSec': f"{count / (now - start): 10.2f}", "ORM": orm}
    result = result.append(query_res, ignore_index=True)
    result.to_csv('sqlalchemy_outputs/test_k.csv')
    print(f"SQLAlchemy Testcase K, Rows deleted: {count}, time taken: {now-start}")
    print(f"SQLAlchemy ORM, K: Rows/sec: {count / (now - start): 10.2f}")


if __name__ == "__main__":
    main()
