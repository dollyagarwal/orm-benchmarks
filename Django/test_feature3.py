# Test B→ Bulk Insert


import os
from warehouses.models import *
from django.db.models import Count, Q, FilteredRelation

import sys

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "mysite.settings")

try:
    import django  # noqa

    django.setup()  # noqa
finally:
    pass


def main():
    print("---------Distinct------")
    print(warehouse.objects.distinct('city').values('city').query)

    print("---------Order By------")
    print(warehouse.objects.filter(city__contains='et').order_by('-city').query)

    print("---------Group By------")
    print(warehouse.objects.values('city').annotate(Count('id')).query)
    print(warehouse.objects.values('city').annotate(id__count=Count('id')).filter(id__count__gt=5).order_by(
        'id__count').query)

    print("-----Joins-----")
    print(district.objects.select_related().query)  ##inner join
    print(warehouse.objects.annotate(dist=FilteredRelation('district', condition=Q(district__tax=3000))).values('city',
                                                                                                                'dist__tax').query)  ##left join
    print(warehouse.objects.annotate(dist=FilteredRelation('district')).values('dist').query)  ##left join

    print("-----Union-----")
    print(warehouse.objects.filter(city='Moscow').union(warehouse.objects.filter(city='Pshkin'), all=True).query)

    print("-----Except-----")
    print(warehouse.objects.filter(city='Moscow').difference(warehouse.objects.filter(tax__gte=500)).query)

    print("-----Intersect-----")
    print(warehouse.objects.filter(city='Moscow').intersection(warehouse.objects.filter(tax__gte=500)).query)


if __name__ == '__main__':
    file_path = 'outputs/test_feature3.txt'
    sys.stdout = open(file_path, "w+")

    main()
