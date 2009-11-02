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
    ARGS '"/home/yanif/datasets/tpch/sf1/singlefile/part.tbl.a",&DBToaster::DemoDatasets::parsePartField,9,2100000,512'
    INSTANCE 'SSBParts'
    TUPLE 'DBToaster::DemoDatasets::part'
    ADAPTOR 'DBToaster::DemoDatasets::PartTupleAdaptor';

select sum(l.extendedprice)
from   lineitem l, parts p
where  p.partkey = l.partkey
and    l.quantity < 0.005*
       (select sum(l2.quantity)
        from lineitem l2 where l2.partkey = p.partkey);
