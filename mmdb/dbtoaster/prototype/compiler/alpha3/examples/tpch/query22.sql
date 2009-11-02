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

select c1.nationkey, sum(c1.acctbal) from customer c1
where c1.acctbal <
    (select sum(c2.acctbal) from customer c2 where c2.acctbal > 0)
and 0 = (select sum(1) from orders o where o.custkey = c1.custkey)
group by c1.nationkey