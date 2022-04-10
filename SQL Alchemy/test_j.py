import time

import pandas as pd
from sqlalchemy.orm import sessionmaker

from models import *
from settings import *

engine = create_engine(PROVIDER['postgres'], echo=False)
Session = sessionmaker(bind=engine)
session = Session()

testcase = 'J'
orm = 'SQLAlchemy'


def main():
    result = pd.DataFrame()
    objs = list(session.query(Item).all())
    count = len(objs)

    start = time.time()

    for obj in objs:
        obj.name = f"{obj.name} Update1"
        session.add(obj)
    session.commit()

    now = time.time()

    query_res = {'Testcase': testcase, 'Table': 'Item', 'Rows': count, 'Total time': f"{(now - start):10.2f}",
                 'RowsPerSec': f"{count / (now - start): 10.2f}", "ORM": orm}
    result = result.append(query_res, ignore_index=True)
    result.to_csv('sqlalchemy_outputs/test_j.csv')


if __name__ == "__main__":
    main()
