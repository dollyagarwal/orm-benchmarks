# Test B→ Bulk Insert


import os
from warehouses.models import *
from django.db.models import Avg, Max, Min, Sum, Count

import sys

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "mysite.settings")

try:
    import django  # noqa

    django.setup()  # noqa
finally:
    pass


def main():
    print("---------Aggregation------")
    print(item.objects.all().aggregate(Max('price')))
    print(item.objects.all().aggregate(Min('price')))
    print(item.objects.all().aggregate(Sum('price')))
    print(item.objects.all().aggregate(Avg('price')))
    print(item.objects.all().aggregate(Count('price')))

    print("---------Regular Expression------")
    print(district.objects.filter(city__regex=r'^(M?|P) +').values('id', 'city').query)


if __name__ == '__main__':
    file_path = 'outputs/test_feature2.txt'
    sys.stdout = open(file_path, "w+")

    main()
