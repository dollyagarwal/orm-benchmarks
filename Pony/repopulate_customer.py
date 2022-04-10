from pony.orm import commit, db_session
from datetime import datetime
from random import choice, randint

import pandas as pd
from pony.orm import commit, db_session

from create_schema import Customer, db

num_rows = 0
data = pd.read_csv("MOCK_DATA.csv")
cities = ['Moscow', 'St. Petersbrg', 'Pshkin', 'Oraneinbaum', 'Vladivostok']
names = data['names '].tolist()
last_names = data.last_name.tolist()
d_cnt = 5000

db.drop_table("customer", with_all_data=True)
db.create_tables()

for i in range(10 * 500):
    num_rows += 1
    with db_session():
        Customer(first_name=choice(names), middle_name=choice(names), last_name=choice(last_names),
                 street_1='c_st %d' % i, street_2='c_st2 %d' % i, city=choice(cities), c_zip='c_zip %d' % i,
                 phone='phone', since=datetime(2005, 7, 14, 12, 30), credit='credit', credit_lim=randint(1000, 100000),
                 discount=choice((0, 10, 15, 20, 30)), delivery_cnt=0, payment_cnt=0, balance=1000000, ytd_payment=0,
                 data1='customer %d' % i, data2='hello %d' % i, district=randint(1, d_cnt))
        commit()
