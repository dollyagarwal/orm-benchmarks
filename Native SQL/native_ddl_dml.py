import psycopg2
import time
import sys
import pandas as pd
from random import choice, randint
from datetime import datetime

con = psycopg2.connect(database='project1', user='postgres',
                       password='password')

file_path = 'outputs/native_ddl.csv'
sys.stdout = open(file_path, "w+")

mock_data = 'MOCK_DATA.csv'
data = pd.read_csv(mock_data)

citys = ['Moscow', 'St. Petersbrg', 'Pshkin', 'Oraneinbaum', 'Vladivostok']
names = data['names'].tolist()
last_names = data['last_name'].tolist()

n = 500

with con:
    cur = con.cursor()

    cur.execute(
        "CREATE TABLE warehouse(id SERIAL PRIMARY KEY, number INT, street_1 VARCHAR(255) NOT NULL, street_2 VARCHAR("
        "255) NOT NULL, city VARCHAR(255) NOT NULL, w_zip VARCHAR(255) NOT NULL, tax FLOAT, ytd FLOAT)")
    cur.execute(
        "CREATE TABLE district (id SERIAL NOT NULL, warehouse_id INTEGER, name VARCHAR NOT NULL, street_1 VARCHAR("
        "255) NOT NULL, street_2 VARCHAR(255) NOT NULL, city VARCHAR(255) NOT NULL, d_zip VARCHAR(255) NOT NULL, "
        "tax FLOAT, ytd FLOAT, PRIMARY KEY (id), FOREIGN KEY(warehouse_id) REFERENCES warehouse (id))")
    cur.execute(
        "CREATE TABLE item(id SERIAL PRIMARY KEY, name VARCHAR(255) NOT NULL, price double precision NOT NULL, "
        "data VARCHAR(255) NOT NULL)")
    cur.execute(
        "CREATE TABLE customer(id SERIAL NOT NULL, first_name VARCHAR NOT NULL, middle_name VARCHAR NOT NULL, "
        "last_name VARCHAR NOT NULL, street_1 VARCHAR NOT NULL, street_2 VARCHAR NOT NULL, city VARCHAR NOT NULL, "
        "c_zip VARCHAR NOT NULL, phone VARCHAR NOT NULL, since TIMESTAMP WITHOUT TIME ZONE NOT NULL, credit VARCHAR "
        "NOT NULL, credit_lim FLOAT NOT NULL, discount FLOAT NOT NULL, delivery_cnt INTEGER NOT NULL, payment_cnt "
        "INTEGER NOT NULL, balance FLOAT NOT NULL, ytd_payment FLOAT NOT NULL, data1 TEXT NOT NULL, data2 TEXT NOT "
        "NULL, district_id INTEGER, PRIMARY KEY(id), FOREIGN KEY(district_id) REFERENCES district(id))")
    cur.execute(
        "CREATE TABLE stock (warehouse_id INTEGER NOT NULL, item_id INTEGER NOT NULL, quantity INTEGER, ytd FLOAT, "
        "order_cnt INTEGER, remote_cnt INTEGER, data VARCHAR NOT NULL, PRIMARY KEY (warehouse_id, item_id), "
        "FOREIGN KEY(warehouse_id) REFERENCES warehouse (id), FOREIGN KEY(item_id) REFERENCES item (id))")

    nrows = 0
    table = []
    rows = []
    times = []
    rowspersec = []

    print("-------------Insert-------------")

    start = time.time()
    for i in range(1, n + 1):
        number = i
        street_1 = 'w_st %d' % i
        street_2 = 'w_st2 %d' % i
        city = choice(citys)
        w_zip = 'w_zip %d' % i
        tax = float(i)
        ytd = 0
        values = str(f"{number},'{street_1}','{street_2}','{city}','{w_zip}',{tax},{ytd}")
        command = f'INSERT INTO warehouse(number,street_1,street_2,city,w_zip,tax,ytd) VALUES({values})'
        cur.execute(command)
        nrows += 1

    now = time.time()
    print(f"Warehouse: Rows: {nrows}, Time: {(now - start): 10.2f}")
    print(f"Warehouse: Rows/sec: {nrows / (now - start): 10.2f}")
    time_taken = now - start
    table.append('Warehouse')
    rows.append(nrows)
    times.append(time_taken)
    rowspersec.append((nrows / time_taken))

    start = now = time.time()
    nrows = 0
    d_cnt = 0
    for i in range(1, n + 1):
        for j in range(10):
            nrows += 1
            warehouse_id = i
            name = 'dist %d %d' % (i, j)
            street_1 = 'd_st %d' % j
            street_2 = 'd_st2 %d' % j
            city = choice(citys)
            d_zip = 'd_zip %d' % j
            tax = float(j)
            ytd = 0
            values = str(f"{warehouse_id},'{name}','{street_1}','{street_2}','{city}','{d_zip}',{tax},{ytd}")
            command = f"INSERT INTO district(warehouse_id,name,street_1,street_2,city,d_zip,tax,ytd) VALUES({values})"
            cur.execute(command)
            d_cnt += 1
    now = time.time()
    print(f"District: Rows: {nrows}, Time: {(now - start): 10.2f}")
    print(f"District: Rows/sec: {nrows / (now - start): 10.2f}")
    time_taken = now - start
    table.append('District')
    rows.append(nrows)
    times.append(time_taken)
    rowspersec.append((nrows / time_taken))

    start = now = time.time()
    nrows = 0

    for i in range(10 * n):
        nrows += 1

        first_name = choice(names).replace("'", '')
        middle_name = choice(names).replace("'", '')
        last_name = choice(last_names).replace("'", '')
        street_1 = 'c_st %d' % i
        street_2 = 'c_st2 %d' % i
        city = choice(citys)
        c_zip = 'c_zip %d' % i
        phone = 'phone'
        since = datetime(2005, 7, 14, 12, 30)
        credit = 'credit'
        credit_lim = randint(1000, 100000)
        discount = choice((0, 10, 15, 20, 30))
        delivery_cnt = 0
        payment_cnt = 0
        balance = 1000000
        ytd_payment = 0
        data1 = 'customer %d' % i
        data2 = 'hello %d' % i
        district_id = randint(1, d_cnt)

        values = str(
            f"'{first_name}','{middle_name}','{last_name}','{street_1}','{street_2}','{city}','{c_zip}','{phone}','{since}','{credit}','{credit_lim}',{discount},{delivery_cnt},{payment_cnt},{balance},{ytd_payment},'{data1}','{data2}',{district_id}")
        command = f"INSERT INTO customer(first_name,middle_name,last_name, street_1,street_2,city,c_zip,phone,since," \
                  f"credit,credit_lim,discount,delivery_cnt,payment_cnt,balance,ytd_payment,data1,data2,district_i" \
                  f"d) VALUES({values}) "
        cur.execute(command)

    now = time.time()
    print(f"Native Testcase A, Customers: Rows: {nrows} , Time: {(now - start)}")
    print(f"Native Testcase A, Customers: Rows/sec: {nrows / (now - start): 10.2f}")
    time_taken = now - start
    table.append('Customer')
    rows.append(nrows)
    times.append(time_taken)
    rowspersec.append((nrows / time_taken))

    start = now = time.time()
    nrows = 0

    for i in range(1, n * 10 + 1):
        nrows += 1
        name = 'item %d' % i
        price = randint(1, 100000)
        data = 'data'
        values = str(f"'{name}',{price},'{data}'")
        command = f"INSERT INTO item(name,price,data) VALUES({values})"
        cur.execute(command)

    now = time.time()
    print(f"Native Testcase A, Items: Rows: {nrows} , Time: {(now - start)}")
    print(f"Native Testcase A, Items: Rows/sec: {nrows / (now - start): 10.2f}")
    time_taken = now - start
    table.append('Item')
    rows.append(nrows)
    times.append(time_taken)
    rowspersec.append((nrows / time_taken))

    start = time.time()
    nrows = 0

    for i in range(1, n * 10 + 1):
        for j in range(1, n + 1):
            nrows += 1
            warehouse_id = j
            item_id = i
            quantity = randint(1, 10)
            ytd = randint(1, 100000)
            order_cnt = 0
            remote_cnt = 0
            data = "data"
            values = str(f"{warehouse_id},{item_id},{quantity},{ytd},{order_cnt},{remote_cnt},'{data}'")
            command = f"INSERT INTO stock(warehouse_id,item_id,quantity,ytd,order_cnt,remote_cnt,data) VALUES({values})"
            cur.execute(command)
    now = time.time()
    print(f"Stock: Rows: {nrows}, Time: {(now - start): 10.2f}")
    print(f"Stock: Rows/sec: {nrows / (now - start): 10.2f}")

    table.append('Stock')
    rows.append(nrows)
    times.append(time_taken)
    rowspersec.append((nrows / time_taken))
    output = pd.DataFrame({'Table': table, 'Rows': rows, 'Total Time': times, 'RowsPerSec': rowspersec})
    output['ORM'] = 'Native'
    output['Testcase'] = 'A'
    output.to_csv('outputs/test_a.csv')
