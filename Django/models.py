from django.db import models
from django.utils import timezone


class warehouse(models.Model):
    number = models.IntegerField()
    street_1 = models.TextField()
    street_2 = models.TextField()
    city = models.TextField()
    w_zip = models.TextField()
    tax = models.FloatField()
    ytd = models.FloatField()

    class Meta:
        db_table = 'warehouse'

    # def publish(self):
    #     self.published_date = timezone.now()
    #     self.save()
    #
    # def __str__(self):
    #     return self.title


class district(models.Model):
    warehouse = models.ForeignKey(warehouse, on_delete=models.CASCADE)
    name = models.TextField()
    street_1 = models.TextField()
    street_2 = models.TextField()
    city = models.TextField()
    d_zip = models.TextField()
    tax = models.FloatField()
    ytd = models.FloatField()

    class Meta:
        db_table = 'district'


class order(models.Model):
    warehouse = models.ForeignKey(warehouse, on_delete=models.CASCADE)
    district = models.ForeignKey(district, on_delete=models.CASCADE)
    ol_cnt = models.IntegerField()
    customer = models.ForeignKey('customer', on_delete=models.CASCADE)
    entry_d = models.DateTimeField(default=timezone.now)
    is_o_delivered = models.BooleanField(default=False)

    class Meta:
        db_table = 'order'


class orderline(models.Model):
    item = models.ForeignKey('item', on_delete=models.CASCADE)
    order = models.ForeignKey(order, on_delete=models.CASCADE)
    delivery_d = models.DateTimeField(default=timezone.now, blank=True, null=True)
    amount = models.IntegerField()

    class Meta:
        db_table = 'orderline'


class stock(models.Model):
    warehouse = models.ForeignKey(warehouse, on_delete=models.CASCADE)
    item = models.ForeignKey('item', on_delete=models.CASCADE)
    quantity = models.IntegerField()
    ytd = models.FloatField()
    order_cnt = models.IntegerField()
    remote_cnt = models.IntegerField()
    data = models.TextField()

    class Meta:
        db_table = 'stock'
        constraints = (models.UniqueConstraint(fields=['warehouse', 'item'], name='unique_stock'),)


class item(models.Model):
    name = models.TextField()
    price = models.FloatField()
    data = models.TextField()

    class Meta:
        db_table = 'item'


class customer(models.Model):
    first_name = models.TextField()
    middle_name = models.TextField()
    last_name = models.TextField()
    street_1 = models.TextField()
    street_2 = models.TextField()
    city = models.TextField()
    c_zip = models.TextField()
    phone = models.TextField()
    since = models.DateTimeField(default=timezone.now)
    credit = models.TextField()
    credit_lim = models.DecimalField(max_digits=12, decimal_places=2)
    discount = models.FloatField()
    delivery_cnt = models.IntegerField()
    payment_cnt = models.IntegerField()
    balance = models.FloatField()
    ytd_payment = models.FloatField()
    data1 = models.TextField()
    data2 = models.TextField()
    district = models.ForeignKey(district, on_delete=models.CASCADE)

    class Meta:
        db_table = 'customer'


class history(models.Model):
    date = models.DateTimeField(default=timezone.now)
    amount = models.FloatField()
    data = models.TextField()
    customer = models.ForeignKey(customer, on_delete=models.CASCADE)

    class Meta:
        db_table = 'history'
