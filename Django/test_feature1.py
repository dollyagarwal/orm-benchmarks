# Test B→ Bulk Insert


import os
from warehouses.models import *
from django.db.models import Q

import sys

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "mysite.settings")

try:
    import django  # noqa

    django.setup()  # noqa
finally:
    pass


def main():
    print("Null Values")
    print(warehouse.objects.filter(city__isnull=True).all().query)
    print(warehouse.objects.exclude(city__isnull=True).all().query)

    print("---------Pattern matching------")
    print(warehouse.objects.filter(city__exact='Moscow').all().query)
    print(warehouse.objects.filter(city__contains='et').all().query)
    print('Case insensitive:-', warehouse.objects.filter(city__startswith='et').all().query)
    print('Case insensitive:-', warehouse.objects.filter(city__endswith='et').all().query)
    print('Case insensitive:-', warehouse.objects.filter(city__icontains='et').all().query)
    print("Pattern not equal to")
    print(warehouse.objects.exclude(city__contains='et').all().query)

    print("--------Range on numbers---------")
    print(stock.objects.filter(quantity__gt=7, quantity__lte=9).all().query)

    print("--------Relational Expressions---------")
    print(item.objects.filter(name__contains='_1', price__gte=100).all().query)
    print(item.objects.filter(name__contains='_1').filter(price__gte=100).all().query)
    print(item.objects.filter(Q(name__contains='_1') & Q(price__gte=100)).all().query)
    print(item.objects.filter(Q(name__contains='_1') | ~Q(price__gte=100)).all().query)


if __name__ == '__main__':
    file_path = 'outputs/test_feature1.txt'
    sys.stdout = open(file_path, "w+")

    main()
