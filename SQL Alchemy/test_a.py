"""
Test A : Single Insert
"""

import time
from random import choice, randint
from models import *
from settings import AMOUNT_OF_WAREHOUSES

from sqlalchemy.orm import sessionmaker

import pandas as pd

testcase = 'A'
orm = 'SQLAlchemy'

mock_data = 'MOCK_DATA.csv'


def populate(n):
    result = pd.DataFrame()
    # print("--------------------Running Test A-------------------")
    # print(f"test done at {datetime.now()}")
    Session = sessionmaker(bind=engine)
    session = Session()

    data = pd.read_csv(mock_data)

    citys = ['Moscow', 'St. Petersbrg', 'Pshkin', 'Oraneinbaum', 'Vladivostok']
    names = data['names'].tolist()
    last_names = data['last_name'].tolist()

    start = now = time.time()
    nrows = 0
    for i in range(1, n + 1):
        nrows += 1
        w = Warehouse(
            number=i,
            street_1='w_st %d' % i,
            street_2='w_st2 %d' % i,
            city=choice(citys),
            w_zip='w_zip %d' % i,
            tax=float(i),
            ytd=0
        )
        session.add(w)

    now = time.time()
    query_res = {'Testcase': testcase, 'Table': 'Warehouse', 'Rows': nrows, 'Total time': f"{(now - start):10.2f}",
                 'RowsPerSec': f"{nrows / (now - start): 10.2f}", 'ORM': orm}
    result = result.append(query_res, ignore_index=True)
    print(f"SQLAlchemy Testcase A, Warehouse: Rows: {nrows}, Time: {(now - start): 10.2f}")
    print(f"SQLAlchemy Testcase A Warehouse: Rows/sec: {nrows / (now - start): 10.2f}")

    start = now = time.time()
    nrows = 0
    d_cnt = 0
    for i in range(1, n + 1):
        for j in range(10):
            nrows += 1
            d = District(
                warehouse_id=i,
                name='dist %d %d' % (w.number, j),
                street_1='d_st %d' % j,
                street_2='d_st2 %d' % j,
                city=w.city,
                d_zip='d_zip %d' % j,
                tax=float(j),
                ytd=0,
            )
            session.add(d)
            d_cnt += 1

    now = time.time()
    query_res = {'Testcase': testcase, 'Table': 'District', 'Rows': nrows, 'Total time': f"{(now - start):10.2f}",
                 'RowsPerSec': f"{nrows / (now - start): 10.2f}", "ORM": orm}
    result = result.append(query_res, ignore_index=True)
    print(f"SQLAlchemy Testcase A, District: Rows: {nrows}, Time: {(now - start): 10.2f}")
    print(f"SQLAlchemy Testcase A District: Rows/sec: {nrows / (now - start): 10.2f}")

    start = now = time.time()
    nrows = 0

    for i in range(10 * n):
        nrows += 1
        c = Customer(
            first_name=choice(names),
            middle_name=choice(names),
            last_name=choice(last_names),
            street_1='c_st %d' % i,
            street_2='c_st2 %d' % i,
            city=choice(citys),
            c_zip='c_zip %d' % i,
            phone='phone',
            since=datetime(2005, 7, 14, 12, 30),
            credit='credit',
            credit_lim=randint(1000, 100000),
            discount=choice((0, 10, 15, 20, 30)),
            delivery_cnt=0,
            payment_cnt=0,
            balance=1000000,
            ytd_payment=0,
            data_1='customer %d' % i,
            data_2='hello %d' % i,
            district_id=randint(1, d_cnt),
        )
        session.add(c)
        session.commit()
    now = time.time()
    query_res = {'Testcase': testcase, 'Table': 'Customer', 'Rows': nrows, 'Total time': f"{(now - start):10.2f}",
                 'RowsPerSec': f"{nrows / (now - start): 10.2f}", 'ORM': orm}
    result = result.append(query_res, ignore_index=True)
    print(f"SQLAlchemy Testcase A, Customer: Rows: {nrows}, Time: {(now - start): 10.2f}")
    print(f"SQLAlchemy Testcase A Customer: Rows/sec: {nrows / (now - start): 10.2f}")

    start = now = time.time()
    nrows = 0

    for i in range(1, n * 10 + 1):
        nrows += 1
        it = Item(
            name='item %d' % i,
            price=randint(1, 100000),
            data='data'
        )
        session.add(it)

    now = time.time()
    query_res = {'Testcase': testcase, 'Table': 'Item', 'Rows': nrows, 'Total time': f"{(now - start):10.2f}",
                 'RowsPerSec': f"{nrows / (now - start): 10.2f}", "ORM": orm}
    result = result.append(query_res, ignore_index=True)
    print(f"SQLAlchemy Testcase A, Item: Rows: {nrows}, Time: {(now - start): 10.2f}")
    print(f"SQLAlchemy Testcase A Item: Rows/sec: {nrows / (now - start): 10.2f}")

    start = now = time.time()
    nrows = 0

    for i in range(1, n * 10 + 1):
        for j in range(1, n + 1):
            nrows += 1
            s = Stock(
                warehouse_id=j,
                item_id=i,
                quantity=randint(1, 10),
                ytd=randint(1, 100000),
                order_cnt=0,
                remote_cnt=0,
                data="data",
            )
            session.add(s)
    session.commit()

    now = time.time()
    query_res = {'Testcase': testcase, 'Table': 'Stock', 'Rows': nrows, 'Total time': f"{(now - start):10.2f}",
                 'RowsPerSec': f"{nrows / (now - start): 10.2f}", "ORM": orm}
    result = result.append(query_res, ignore_index=True)
    print(f"SQLAlchemy Testcase A, Stock: Rows: {nrows}, Time: {(now - start): 10.2f}")
    print(f"SQLAlchemy Testcase A Stock: Rows/sec: {nrows / (now - start): 10.2f}")
    print(result)
    result.to_csv('sqlalchemy_outputs/test_a.csv', encoding='utf-8')


def main():
    with engine.connect() as con:
        con.execution_options(autocommit=True).execute('TRUNCATE TABLE "warehouse" RESTART IDENTITY cascade')
        con.execution_options(autocommit=True).execute('TRUNCATE TABLE "district" RESTART IDENTITY cascade')
        con.execution_options(autocommit=True).execute('TRUNCATE TABLE "customer" RESTART IDENTITY cascade')
        con.execution_options(autocommit=True).execute('TRUNCATE TABLE "stock" RESTART IDENTITY cascade')
        con.execution_options(autocommit=True).execute('TRUNCATE TABLE "item" RESTART IDENTITY cascade')

    start = now = time.time()
    populate(AMOUNT_OF_WAREHOUSES)
    now = time.time()
    print(f"Test A SQLAlchemy: total runtime: {(now - start): 10.2f}")


if __name__ == "__main__":
    main()
