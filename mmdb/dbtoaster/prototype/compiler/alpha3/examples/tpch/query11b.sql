CREATE TABLE PARTSUPP (
        partkey      bigint,
        suppkey      bigint,
        availqty     int,
        supplycost   double,
        comment      text
    )
    FROM 'file'
    SOURCE 'DBToaster::DemoDatasets::PartSuppStream'
    ARGS '"/Users/yanif/datasets/tpch/sf1/singlefile/partsupp.tbl.a",&DBToaster::DemoDatasets::parsePartSuppField,5,820000,512'
    INSTANCE 'SSBPartSupp'
    TUPLE 'DBToaster::DemoDatasets::partsupp'
    ADAPTOR 'DBToaster::DemoDatasets::PartSuppTupleAdaptor';

CREATE TABLE SUPPLIER (
        suppkey      bigint,
        name         text,
        address      text,
        nationkey    bigint,
        phone        text,
        acctbal      double,
        comment      text
    )
    FROM 'file'
    SOURCE 'DBToaster::DemoDatasets::SupplierStream'
    ARGS '"/Users/yanif/datasets/tpch/sf1/singlefile/supplier.tbl.a",&DBToaster::DemoDatasets::parseSupplierField,7,110000,512'
    INSTANCE 'SSBSupplier'
    TUPLE 'DBToaster::DemoDatasets::supplier'
    ADAPTOR 'DBToaster::DemoDatasets::SupplierTupleAdaptor';

select sum(ps.supplycost * ps.availqty)
from  partsupp ps, supplier s
where ps.suppkey = s.suppkey
