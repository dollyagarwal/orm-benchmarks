from peewee import (
    DecimalField,
    DateTimeField,
    FloatField,
    ForeignKeyField,
    IntegerField,
    Model,
    BooleanField,
    TextField,
)

from playhouse.postgres_ext import PostgresqlExtDatabase

db = PostgresqlExtDatabase("project", user="postgres", password="password", host="localhost", port=5432)


class Warehouse(Model):
    id = IntegerField(primary_key=True)
    number = IntegerField(null=True)
    street_1 = TextField(null=True)
    street_2 = TextField(null=True)
    city = TextField(null=True)
    w_zip = TextField(null=True)
    tax = FloatField(null=True)
    ytd = FloatField(null=True)

    class Meta:
        database = db


class District(Model):
    id = IntegerField(primary_key=True)
    warehouse = ForeignKeyField(Warehouse, backref="districts")
    name = TextField(null=True)
    street_1 = TextField(null=True)
    street_2 = TextField(null=True)
    city = TextField(null=True)
    d_zip = TextField(null=True)
    tax = FloatField(null=True)
    ytd = FloatField(null=True)

    class Meta:
        database = db


class Item(Model):
    id = IntegerField(primary_key=True)
    name = TextField(null=True)
    price = FloatField(null=True)
    data = TextField(null=True)

    class Meta:
        database = db


class Stock(Model):
    id = IntegerField(primary_key=True)
    warehouse = ForeignKeyField(Warehouse, backref="orders")
    item = ForeignKeyField(Item, backref="stocks")
    quantity = IntegerField(null=True)
    ytd = FloatField(null=True)
    order_cnt = IntegerField(null=True)
    remote_cnt = IntegerField(null=True)
    data = TextField(null=True)

    class Meta:
        database = db


class Customer(Model):
    id = IntegerField(primary_key=True)
    district = ForeignKeyField(District, backref="customers")
    first_name = TextField(null=True)
    middle_name = TextField()
    last_name = TextField(null=True)
    street_1 = TextField(null=True)
    street_2 = TextField(null=True)
    city = TextField(null=True)
    c_zip = TextField(null=True)
    phone = TextField(null=True)
    since = DateTimeField(null=True)
    credit = TextField(null=True)
    credit_lim = DecimalField(null=True)
    discount = FloatField(null=True)
    delivery_cnt = IntegerField(null=True)
    payment_cnt = IntegerField(null=True)
    balance = FloatField(null=True)
    ytd_payment = FloatField(null=True)
    data_1 = TextField(null=True)
    data_2 = TextField(null=True)

    class Meta:
        database = db


class Order(Model):
    id = IntegerField(primary_key=True)
    warehouse = ForeignKeyField(Warehouse, backref="orders")
    district = ForeignKeyField(District, backref="orders")
    ol_cnt = IntegerField(null=True)
    entry_d = DateTimeField(null=True)
    is_o_delivered = BooleanField(null=True)
    customer = ForeignKeyField(Customer, backref="orders")

    class Meta:
        database = db


class OrderLine(Model):
    id = IntegerField(primary_key=True)
    item = ForeignKeyField(Item, backref="order_lines")
    delivery_d = DateTimeField(null=True)
    amount = IntegerField(null=True)
    order = ForeignKeyField(Order, backref="order")

    class Meta:
        database = db


class History(Model):
    id = IntegerField(primary_key=True)
    customer = ForeignKeyField(Customer, backref="history")
    date = DateTimeField(null=True)
    amount = FloatField(null=True)
    data = TextField(null=True)

    class Meta:
        database = db


TABLES = [Warehouse, District, Order, Stock, Item, OrderLine, Customer, History]


def create_tables():
    if not db.get_tables():
        with db:
            db.create_tables(TABLES)
    else:
        for table in TABLES:
            table.truncate_table(restart_identity=True, cascade=True)
