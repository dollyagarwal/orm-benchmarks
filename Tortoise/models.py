from tortoise.models import Model
from tortoise import fields


class item(Model):
    name = fields.TextField()
    price = fields.FloatField(null=True)
    data = fields.TextField()

    class Meta:
        table = "item"


class warehouse(Model):
    number = fields.IntField(null=True)
    street_1 = fields.TextField()
    street_2 = fields.TextField()
    city = fields.TextField()
    w_zip = fields.TextField()
    tax = fields.FloatField(null=True)
    ytd = fields.FloatField(null=True)

    class Meta:
        table = "warehouse"

    def __str__(self):
        return self.city


class district(Model):
    warehouse = fields.ForeignKeyField('models.warehouse')
    name = fields.TextField()
    street_1 = fields.TextField()
    street_2 = fields.TextField()
    city = fields.TextField()
    d_zip = fields.TextField()
    tax = fields.FloatField(null=True)
    ytd = fields.FloatField(null=True)

    class Meta:
        table = "district"
        # indexes = ('warehouse')


class customer(Model):
    district = fields.ForeignKeyField('models.district')
    first_name = fields.TextField()
    middle_name = fields.TextField()
    last_name = fields.TextField()
    street_1 = fields.TextField()
    street_2 = fields.TextField()
    city = fields.TextField()
    c_zip = fields.TextField()
    phone = fields.TextField()
    since = fields.DatetimeField(null=True)
    credit = fields.TextField()
    credit_lim = fields.DecimalField(12, 2, null=True)
    discount = fields.FloatField(null=True)
    delivery_cnt = fields.IntField(null=True)
    payment_cnt = fields.IntField(null=True)
    balance = fields.FloatField(null=True)
    ytd_payment = fields.FloatField(null=True)
    data_1 = fields.TextField()
    data_2 = fields.TextField()

    class Meta:
        table = "customer"


class history(Model):
    customer = fields.ForeignKeyField('models.customer')
    date = fields.DatetimeField(null=True)
    amount = fields.FloatField(null=True)
    data = fields.TextField()

    class Meta:
        table = "history"
        # indexes = ('customer')


class order(Model):
    warehouse = fields.ForeignKeyField('models.warehouse')
    district = fields.ForeignKeyField('models.district')
    customer = fields.ForeignKeyField('models.customer')
    ol_cnt = fields.IntField(null=True)
    entry_d = fields.DatetimeField(null=True)
    is_o_delivered = fields.BooleanField(null=True)

    class Meta:
        table = "order"
        indexes = (("warehouse_id"), ("district_id"), ("customer_id"))


class orderline(Model):
    item = fields.ForeignKeyField('models.item')
    order = fields.ForeignKeyField('models.order')
    delivery_d = fields.DatetimeField(null=True)
    amount = fields.IntField(null=True)

    class Meta:
        table = "orderline"
        indexes = (("item_id"), ("order_id"))


class stock(Model):
    warehouse = fields.ForeignKeyField('models.warehouse')
    item = fields.ForeignKeyField('models.item')
    quantity = fields.IntField(null=True)
    ytd = fields.FloatField(null=True)
    order_cnt = fields.IntField(null=True)
    remote_cnt = fields.IntField(null=True)
    data = fields.TextField()

    class Meta:
        table = "stock"
        unique_together = ("warehouse_id", "item_id")


class event(Model):
    id = fields.IntField(pk=True)
    name = fields.TextField()
    datetime = fields.DatetimeField(null=True)

    class Meta:
        table = "event"

    def __str__(self):
        return self.name


class Author(Model):
    name = fields.CharField(max_length=255)


class Book(Model):
    name = fields.CharField(max_length=255)
    author = fields.ForeignKeyField("models.Author", related_name="books")
    rating = fields.FloatField()
