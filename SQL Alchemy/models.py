import sqlalchemy
from sqlalchemy import Column, DateTime, Integer, String, create_engine, ForeignKey, Float, \
    Boolean, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from settings import PROVIDER
from sqlalchemy.schema import PrimaryKeyConstraint

engine = create_engine(PROVIDER['postgres'], echo=False)

Base = declarative_base()


class Warehouse(Base):
    __tablename__ = 'warehouse'

    id = Column(Integer, primary_key=True)
    number = Column(Integer)
    street_1 = Column(String(255), nullable=False)
    street_2 = Column(String(255), nullable=False)
    city = Column(String(255), nullable=False)
    w_zip = Column(String(255), nullable=False)
    tax = Column(Float)
    ytd = Column(Float)

    orders = relationship("Order", backref='warehouse', lazy='dynamic', cascade='delete')
    districts = relationship("District", backref="warehouse", lazy='dynamic', cascade='delete')
    stocks = relationship("Stock", backref="warehouse", lazy='dynamic', cascade='delete')


class District(Base):
    __tablename__ = 'district'

    id = Column(Integer, primary_key=True)
    warehouse_id = Column(Integer, ForeignKey('warehouse.id'))
    name = Column(String, nullable=False)
    street_1 = Column(String(255), nullable=False)
    street_2 = Column(String(255), nullable=False)
    city = Column(String(255), nullable=False)
    d_zip = Column(String(255), nullable=False)
    tax = Column(Float)
    ytd = Column(Float)

    orders = relationship("Order", backref='district', lazy='dynamic', cascade='delete')
    customers = relationship("Customer", backref='district', lazy='dynamic', cascade='delete')


class Customer(Base):
    __tablename__ = 'customer'

    id = Column(Integer, primary_key=True)
    first_name = Column(String, nullable=False)
    middle_name = Column(String, nullable=False)
    last_name = Column(String, nullable=False)
    street_1 = Column(String, nullable=False)
    street_2 = Column(String, nullable=False)
    city = Column(String, nullable=False)
    c_zip = Column(String, nullable=False)
    phone = Column(String, nullable=False)
    since = Column(DateTime, nullable=False)
    credit = Column(String, nullable=False)
    credit_lim = Column(Float, nullable=False)
    discount = Column(Float, nullable=False)
    delivery_cnt = Column(Integer, nullable=False)
    payment_cnt = Column(Integer, nullable=False)
    balance = Column(Float, nullable=False)
    ytd_payment = Column(Float, nullable=False)
    data_1 = Column(Text, nullable=False)
    data_2 = Column(Text, nullable=False)
    district_id = Column(Integer, ForeignKey('district.id'), index=True)
    orders = relationship("Order", backref='customer', lazy='dynamic', cascade='delete')
    history = relationship("History", backref='customer', lazy='dynamic', cascade='delete')


class Stock(Base):
    __tablename__ = 'stock'
    # id = Column(Integer, primary_key = True)
    warehouse_id = Column(Integer, ForeignKey('warehouse.id'))
    item_id = Column(Integer, ForeignKey('item.id'))
    quantity = Column(Integer)
    ytd = Column(Float)
    order_cnt = Column(Integer)
    remote_cnt = Column(Integer)
    data = Column(String, nullable=False)
    __table_args__ = (
        PrimaryKeyConstraint(
            warehouse_id,
            item_id),
        {})


class Item(Base):
    __tablename__ = 'item'

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    price = Column(Float)
    data = Column(String(255), nullable=False)

    stocks = relationship('Stock', backref='item', lazy='dynamic', cascade='delete')
    order_lines = relationship("OrderLine", backref='item', lazy='dynamic', cascade='delete')


class Order(Base):
    __tablename__ = 'order'

    id = Column(Integer, primary_key=True)
    warehouse_id = Column(Integer, ForeignKey('warehouse.id'))
    district_id = Column(Integer, ForeignKey('district.id'))
    customer_id = Column(Integer, ForeignKey('customer.id'))
    ol_cnt = Column(Integer)
    entry_id = Column(DateTime)
    is_o_delivered = Column(Boolean)

    order_lines = relationship("OrderLine", backref="order", lazy='dynamic', cascade='delete')


class OrderLine(Base):
    __tablename__ = 'orderline'

    id = Column(Integer, primary_key=True)
    delivery_d = Column(DateTime)
    amount = Column(Integer)

    item_id = Column(Integer, ForeignKey('item.id'))
    order_id = Column(Integer, ForeignKey('order.id'))


class History(Base):
    __tablename__ = 'history'

    id = Column(Integer, primary_key=True)
    customer_id = Column(Integer, ForeignKey('customer.id'))
    date = Column(DateTime)
    amount = Column(Float)
    data = Column(String(255), nullable=False)


def drop_tables():
    insp = sqlalchemy.inspect(engine)
    for table_entry in reversed(insp.get_sorted_table_and_fkc_names()):
        table_name = table_entry[0]
        if table_name:
            with engine.begin() as conn:
                conn.execute(sqlalchemy.text(f'DROP TABLE "{table_name}"'))


def create_tables():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
