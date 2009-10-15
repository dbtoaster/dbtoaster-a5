CREATE TABLE LINEITEM (
        orderkey       bigint,
        partkey        bigint,
        suppkey        bigint,
        linenumber     int,
        quantity       double,
        extendedprice  double,
        discount       double,
        tax            double,
        returnflag     text,
        linestatus     text,
        -- use text for dates
        shipdate       text,
        commitdate     text,
        receiptdate    text,
        -- end dates
        shipinstruct   text,
        shipmode       text,
        comment        text
    )
    FROM 'file'
    SOURCE 'DBToaster::DemoDatasets::LineitemStream'
    ARGS '"/Users/yanif/datasets/tpch/sf1/singlefile/lineitem.tbl.a",&DBToaster::DemoDatasets::parseLineitemField,16,65000000,512'
    INSTANCE 'SSBLineitem'
    TUPLE 'DBToaster::DemoDatasets::lineitem'
    ADAPTOR 'DBToaster::DemoDatasets::LineitemTupleAdaptor';

CREATE TABLE ORDERS (
	orderkey       bigint,
        custkey        bigint,
	orderstatus    text,
        totalprice     double,
	orderdate      text, -- date
        orderpriority  text,
	clerk          text,
        shippriority   text,
	comment        text
    )
    FROM 'file'
    SOURCE 'DBToaster::DemoDatasets::OrderStream'
    ARGS '"/Users/yanif/datasets/tpch/sf1/singlefile/orders.tbl.a",&DBToaster::DemoDatasets::parseOrderField,9,17000000,512'
    INSTANCE 'SSBOrder'
    TUPLE 'DBToaster::DemoDatasets::order'
    ADAPTOR 'DBToaster::DemoDatasets::OrderTupleAdaptor';


CREATE TABLE PARTS (
	partkey      bigint,
        name         text,
	mfgr         text,
        brand        text,
	type         text,
        size         integer,
	container    text,
        retailprice  double,
	comment      text
    )
    FROM 'file'
    SOURCE 'DBToaster::DemoDatasets::PartStream'
    ARGS '"/Users/yanif/datasets/tpch/sf1/singlefile/part.tbl.a",&DBToaster::DemoDatasets::parsePartField,9,2100000,512'
    INSTANCE 'SSBParts'
    TUPLE 'DBToaster::DemoDatasets::part'
    ADAPTOR 'DBToaster::DemoDatasets::PartTupleAdaptor';

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
    ARGS '"/Users/yanif/datasets/tpch/sf1/singlefile/customer.tbl.a",&DBToaster::DemoDatasets::parseCustomerField,8,1600000, 512'
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
    ARGS '"/Users/yanif/datasets/tpch/sf1/singlefile/supplier.tbl.a",&DBToaster::DemoDatasets::parseSupplierField,7,110000,512'
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
    ARGS '"/Users/yanif/datasets/tpch/sf1/singlefile/region.tbl",&DBToaster::DemoDatasets::parseRegionField,3,100,512'
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
    ARGS '"/Users/yanif/datasets/tpch/sf1/singlefile/nation.tbl",&DBToaster::DemoDatasets::parseSupplierField,3,100,512'
    INSTANCE 'SSBNation'
    TUPLE 'DBToaster::DemoDatasets::nation'
    ADAPTOR 'DBToaster::DemoDatasets::NationTupleAdaptor';

SELECT PS.suppkey, sum(PS.availqty)
    FROM PARTS P, PARTSUPP PS
    WHERE P.partkey = PS.partkey
    GROUP BY PS.suppkey;