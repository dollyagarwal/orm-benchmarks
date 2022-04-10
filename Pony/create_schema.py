from pony.orm import *
from datetime import datetime
from decimal import Decimal

PROVIDER = {
    'postgres': {
        'provider': 'postgres',
        'user': 'postgres',
        'password': 'password',
        'host': 'localhost',
        'database': 'project'},
}

db = Database(**PROVIDER['postgres'])


class Warehouse(db.Entity):
    number = Required(int)
    street_1 = Required(str)
    street_2 = Required(str)
    city = Required(str)
    w_zip = Required(str)
    tax = Required(float)
    ytd = Required(float)

    orders = Set("Order")
    districts = Set("District")
    stocks = Set("Stock")


class District(db.Entity):
    warehouse_id = Required(Warehouse)
    name = Required(str)
    street_1 = Required(str)
    street_2 = Required(str)
    city = Required(str)
    d_zip = Required(str)
    tax = Required(float)
    ytd = Required(float)
    orders = Set("Order")
    customers = Set("Customer")


class Customer(db.Entity):
    first_name = Required(str)
    middle_name = Required(str)
    last_name = Required(str)
    street_1 = Required(str)
    street_2 = Required(str)
    city = Required(str)
    c_zip = Required(str)
    phone = Required(str)
    since = Required(datetime)
    credit = Required(str)
    credit_lim = Required(Decimal)
    discount = Required(float)
    delivery_cnt = Required(int)
    payment_cnt = Required(int)
    balance = Required(float)
    ytd_payment = Required(float)
    # data1 = Required(LongStr)
    # data2 = Required(LongStr)
    district_id = Required(District)
    orders = Set("Order")
    history = Set("History")


class Stock(db.Entity):
    warehouse_id = Required(Warehouse)
    item_id = Required("Item")
    quantity = Required(int)
    ytd = Required(float)
    order_cnt = Required(int)
    remote_cnt = Required(int)
    data = Required(str)


class Item(db.Entity):
    name = Required(str)
    price = Required(float)
    data = Required(str)
    stock = Set(Stock)
    o_lns = Set("OrderLine")


class Order(db.Entity):
    warehouse_id = Required(Warehouse)
    district_id = Required(District)
    ol_cnt = Required(int)
    customer_id = Required(Customer)
    entry_d = Required(datetime)
    is_o_delivered = Required(bool, default=False)
    o_lns = Set("OrderLine")


class OrderLine(db.Entity):
    delivery_d = Optional(datetime)
    item_id = Required(Item)
    amount = Required(int)
    order_id = Required(Order)


class History(db.Entity):
    date = Required(datetime)
    amount = Required(float)
    data = Required(str)
    customer_id = Required(Customer)


db.generate_mapping(create_tables=False)
