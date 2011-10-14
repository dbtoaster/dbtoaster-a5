# Parses a unified csv from a TPCH query and rewrites in a wide-schema agenda
# format.

from csv import DictWriter
from itertools import chain

from sys import argv

schemas = {
    'LINEITEM': ['orderkey', 'partkey', 'suppkey', 'linenumber', 'quantity',
                 'extendedprice', 'discount', 'tax', 'returnflag', 'linestatus',
                 'shipdate', 'commitdate', 'receiptdate', 'shipinstruct',
                 'shipmode', 'comment'],

    'ORDERS': ['orderkey', 'custkey', 'orderstatus', 'totalprice', 'orderdate',
               'orderpriority', 'clerk', 'shippriority', 'comment'],

    'CUSTOMER': ['custkey', 'name', 'address', 'nationkey', 'phone', 'acctbal',
                 'mktsegment', 'comment'],

    'NATION': ['nationkey', 'name', 'regionkey', 'comment'],

    'PARTSUPP': ['partkey', 'suppkey', 'availqty', 'supplycost', 'comment'],

    'PART': ['partkey', 'name', 'mfgr', 'brand', 'type', 'size', 'container', 'retailprice',
             'comment'],

    'REGION': ['regionkey', 'name', 'comment'],

    'SUPPLIER': ['suppkey', 'name', 'address', 'nationkey', 'phone', 'acctbal', 'comment'],
}

schemas['AGENDA'] = list(sorted(set(chain(*schemas.values()))))

schemas = {t : ['schema', 'event'] + schemas[t] for t in schemas}

if __name__ == '__main__':
    source = open(argv[1])
    agenda_writer = DictWriter(open(argv[2], 'w'), schemas['AGENDA'], restval='')

    for line in source:
        fields = line.strip().split(',')
        insert_dict = dict(zip(schemas[fields[0]], fields))

        agenda_writer.writerow(insert_dict)
