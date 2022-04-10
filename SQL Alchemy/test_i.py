import time
from random import randint

import pandas as pd
from sqlalchemy.orm import sessionmaker

from models import *
from settings import *

engine = create_engine(PROVIDER['postgres'], echo=False)
Session = sessionmaker(bind=engine)
session = Session()

testcase = 'I'
orm = 'SQLAlchemy'


def main():
    result = pd.DataFrame()
    objs = list(session.query(Item).all())
    count = len(objs)

    start = time.time()

    id_count = 5001

    for obj in objs:
        obj.name = f"{obj.name}1"
        obj.price = randint(1, 100000)
        obj.data = f"{obj.data}1"
        session.add(obj)
    session.commit()

    now = time.time()

    query_res = {'Testcase': testcase, 'Table': 'Item', 'Rows': count, 'Total time': f"{(now - start):10.2f}",
                 'RowsPerSec': f"{count / (now - start): 10.2f}", "ORM": orm}
    result = result.append(query_res, ignore_index=True)
    result.to_csv('sqlalchemy_outputs/test_i.csv')


if __name__ == "__main__":
    main()
