CREATE TABLE LINEITEM (
        orderkey       bigint,
        partkey        bigint,
        suppkey        bigint,
        linenumber     int,
        quantity       double,
        extendedprice  double,
        discount       double,
        tax            double
    )
    FROM 'file'
    SOURCE 'DBToaster::DemoDatasets::LineitemStream'
    ARGS '"/home/yanif/datasets/tpch/sf1/singlefile/lineitem.tbl.a",&DBToaster::DemoDatasets::parseLineitemField,16,65000000,512'
    INSTANCE 'SSBLineitem'
    TUPLE 'DBToaster::DemoDatasets::lineitem'
    ADAPTOR 'DBToaster::DemoDatasets::LineitemSimpleTupleAdaptor';

CREATE TABLE ORDERS (
        orderkey       bigint,
        custkey        bigint,
        orderstatus    text,
        totalprice     double,
        orderdate      text, -- date
        orderpriority  text,
        clerk          text,
        shippriority   integer,
        comment        text
    )
    FROM 'file'
    SOURCE 'DBToaster::DemoDatasets::OrderStream'
    ARGS '"/home/yanif/datasets/tpch/sf1/singlefile/orders.tbl.a",&DBToaster::DemoDatasets::parseOrderField,9,17000000,512'
    INSTANCE 'SSBOrder'
    TUPLE 'DBToaster::DemoDatasets::order'
    ADAPTOR 'DBToaster::DemoDatasets::OrderTupleAdaptor';

CREATE TABLE CUSTOMER (
        custkey      bigint,
        name         text,
        address      text,
        nationkey    bigint,
        phone        text,
        acctbal      double,
        mktsegment   text,
        comment      text
    )
    FROM 'file'
    SOURCE 'DBToaster::DemoDatasets::CustomerStream'
    ARGS '"/home/yanif/datasets/tpch/sf1/singlefile/customer.tbl.a",&DBToaster::DemoDatasets::parseCustomerField,8,1600000, 512'
    INSTANCE 'SSBCustomer'
    TUPLE 'DBToaster::DemoDatasets::customer'
    ADAPTOR 'DBToaster::DemoDatasets::CustomerTupleAdaptor';

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
    ARGS '"/home/yanif/datasets/tpch/sf1/singlefile/supplier.tbl.a",&DBToaster::DemoDatasets::parseSupplierField,7,110000,512'
    INSTANCE 'SSBSupplier'
    TUPLE 'DBToaster::DemoDatasets::supplier'
    ADAPTOR 'DBToaster::DemoDatasets::SupplierTupleAdaptor';

CREATE TABLE REGION (
        regionkey    bigint,
        name         text,
        comment      text
    )
    FROM 'file'
    SOURCE 'DBToaster::DemoDatasets::RegionStream'
    ARGS '"/home/yanif/datasets/tpch/sf1/singlefile/region.tbl",&DBToaster::DemoDatasets::parseRegionField,3,100,512'
    INSTANCE 'SSBRegion'
    TUPLE 'DBToaster::DemoDatasets::region'
    ADAPTOR 'DBToaster::DemoDatasets::RegionTupleAdaptor';

CREATE TABLE NATION (
        nationkey    bigint,
        name         text,
        regionkey    bigint,
        comment      text
    )
    FROM 'file'
    SOURCE 'DBToaster::DemoDatasets::NationStream'
    ARGS '"/home/yanif/datasets/tpch/sf1/singlefile/nation.tbl",&DBToaster::DemoDatasets::parseNationField,4,100,512'
    INSTANCE 'SSBNation'
    TUPLE 'DBToaster::DemoDatasets::nation'
    ADAPTOR 'DBToaster::DemoDatasets::NationTupleAdaptor';




select n.name, sum(l.extendedprice * (1 + -1*l.discount))
from customer c, orders o, lineitem l, supplier s, nation n, region r
where c.custkey = o.custkey
and l.orderkey  = o.orderkey
and l.suppkey   = s.suppkey
and c.nationkey = s.nationkey
and s.nationkey = n.nationkey
and n.regionkey = r.regionkey
group by n.name