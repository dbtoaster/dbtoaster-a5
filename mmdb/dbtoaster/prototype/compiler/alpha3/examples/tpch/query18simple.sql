CREATE TABLE LINEITEM (
        orderkey       bigint,
        quantity       double
    )
    FROM 'file'
    SOURCE 'DBToaster::DemoDatasets::LineitemStream'
    ARGS '"/Users/yanif/datasets/tpch/sf1/singlefile/lineitem.tbl.a",&DBToaster::DemoDatasets::parseLineitemField,16,65000000,512'
    INSTANCE 'SSBLineitem'
    TUPLE 'DBToaster::DemoDatasets::lineitem'
    ADAPTOR 'DBToaster::DemoDatasets::LineitemSimpleTupleAdaptor';

CREATE TABLE ORDERS (
        orderkey       bigint,
        custkey        bigint
    )
    FROM 'file'
    SOURCE 'DBToaster::DemoDatasets::OrderStream'
    ARGS '"/Users/yanif/datasets/tpch/sf1/singlefile/orders.tbl.a",&DBToaster::DemoDatasets::parseOrderField,9,17000000,512'
    INSTANCE 'SSBOrder'
    TUPLE 'DBToaster::DemoDatasets::order'
    ADAPTOR 'DBToaster::DemoDatasets::OrderTupleAdaptor';

CREATE TABLE CUSTOMER (
        custkey      bigint
    )
    FROM 'file'
    SOURCE 'DBToaster::DemoDatasets::CustomerStream'
    ARGS '"/Users/yanif/datasets/tpch/sf1/singlefile/customer.tbl.a",&DBToaster::DemoDatasets::parseCustomerField,8,1600000, 512'
    INSTANCE 'SSBCustomer'
    TUPLE 'DBToaster::DemoDatasets::customer'
    ADAPTOR 'DBToaster::DemoDatasets::CustomerTupleAdaptor';

select sum(1)
from customer c, orders o, lineitem l1
where 1 <=
      (select sum(1) from lineitem l2
       where l1.orderkey = l2.orderkey)
and c.custkey = o.custkey
and o.orderkey = l1.orderkey;
--group by c.custkey;