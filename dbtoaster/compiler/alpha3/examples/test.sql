--
--
-- Schema

-- schema for chain join
CREATE TABLE R (A int, B int);
CREATE TABLE S (B int, C int);
CREATE TABLE T (C int, D int);
CREATE TABLE U (D int, E int);

-- schema for star join
CREATE TABLE R1 (A int, W int);
CREATE TABLE S1 (B int, X int);
CREATE TABLE T1 (C int, Y int);
CREATE TABLE U1 (D int, Z int);
CREATE TABLE V1 (A int, B int, C int, D int);

CREATE TABLE TPART (
    partkey int,      name text,
    mfgr text,        brand text,
    type text,        size integer,
    container text,   retailprice float,
    comment text
);

CREATE TABLE TPARTSUPP (
    partkey int,
    suppkey int,
    availqty int,
    supplycost float,
    comment text
);

CREATE TABLE BIDS (T int, ID int, BROKER_ID int, P double, V double);

--
--
-- Parser tests

-- keywords, table and field names are auto-uppercased
SELECT PS.suppkey, sum(PS.availqty)
    FROM TPART P, TPARTSUPP PS
    WHERE P.partkey = PS.partkey
    GROUP BY PS.suppkey;

select PS.suppkey, sum(PS.availqty)
    from tpart P, tpartsupp PS
    where P.partkey = PS.partkey
    group by ps.suppkey;

select PS.SUPPKEY, sum(PS.availQty)
    from tpart P, tpartsupp PS
    where P.partKEY = ps.partkey
    group by ps.suPPkey;

-- fields can be specified without relation name when unambiguous
select suppkey, sum(availqty)
    from tpart p, tpartsupp ps
    where p.partkey = ps.partkey
    group by suppkey;

-- ambiguous fields must be qualified
select ps.partkey, sum(availqty)
    from tpart p, tpartsupp ps
    where p.partkey = ps.partkey
    group by ps.partkey;


/*
--
-- queries that should fail...

-- no attribute E exists
SELECT sum(E) FROM R,S,T WHERE R.B=S.B AND S.C=T.C;

-- no attribute S.D
SELECT sum(A*D) FROM R,S,T WHERE R.B=S.B AND S.D=T.D;

-- no attribute PS.supplykey
select PS.supplykey, sum(PS.availqty)
    from part P, partsupp PS
    where P.partkey = PS.partkey
    group by ps.suppkey;

-- ambiguous fields
select partkey, sum(availqty)
    from part p, partsupp ps
    where p.partkey = ps.partkey
    group by partkey;

-- fields cannot refer to original name when aliased
SELECT TPARTSUPP.suppkey, sum(PS.availqty)
    FROM TPART P, TPARTSUPP PS
    WHERE P.partkey = PS.partkey
    GROUP BY PS.suppkey;

*/


--
--
-- Compilation tests

-- equijoin
SELECT sum(A*C) FROM R,S     WHERE R.B=S.B;
SELECT sum(A*D) FROM R,S,T   WHERE R.B=S.B AND S.C=T.C;
SELECT sum(A*E) FROM R,S,T,U WHERE R.B=S.B AND S.C=T.C AND T.D=U.D;

-- range join
SELECT sum(A*C) FROM R,S     WHERE R.B<S.B;
SELECT sum(A*D) FROM R,S,T   WHERE R.B<S.B AND S.C<T.C;
SELECT sum(A*E) FROM R,S,T,U WHERE R.B<S.B AND S.C<T.C AND T.D<U.D;

-- mix
SELECT sum(A*C) FROM R,S     WHERE R.B<>S.B;
SELECT sum(A*D) FROM R,S,T   WHERE R.B=S.B AND S.C<T.C;
SELECT sum(A*E) FROM R,S,T,U WHERE R.B>S.B AND S.C<>T.C AND T.D=U.D;

-- star
SELECT sum(W*X)     FROM R1,S1,V1       WHERE R1.A=V1.A AND S1.B=V1.B;
SELECT sum(W*X*Y)   FROM R1,S1,T1,V1    WHERE R1.A=V1.A AND S1.B=V1.B and T1.C=V1.C;
SELECT sum(W*X*Y*Z) FROM R1,S1,T1,U1,V1 WHERE R1.A=V1.A AND S1.B=V1.B and T1.C=V1.C and U1.D=V1.D;

-- group bys
SELECT sum(A)   FROM R                                 GROUP BY R.B;
SELECT sum(A*C) FROM R,S     WHERE R.B=S.B             GROUP BY R.A;
SELECT sum(A)   FROM R,S,T   WHERE R.B=S.B AND S.C=T.C GROUP BY T.D;
SELECT sum(A*D) FROM R,S,T   WHERE R.B=S.B AND S.C=T.C GROUP BY S.B;
SELECT sum(A*D) FROM R,S,T   WHERE R.B=S.B AND S.C=T.C GROUP BY S.B, S.C;

-- count, group by
SELECT sum(1)   FROM R                                 GROUP BY R.B;
SELECT sum(1)   FROM R,S     WHERE R.B=S.B             GROUP BY R.A;
SELECT sum(1)   FROM R,S     WHERE R.B=S.B             GROUP BY R.B;
SELECT sum(1)   FROM R,S     WHERE R.B=S.B             GROUP BY R.A, R.B;
SELECT sum(1)   FROM R,S     WHERE R.A=S.C AND R.B=S.B GROUP BY R.B;
SELECT sum(1)   FROM R,S     WHERE R.A<S.C AND R.B=S.B GROUP BY R.B;
SELECT sum(1)   FROM R,S     WHERE R.A<S.C AND R.B=S.B GROUP BY R.A, R.B;


--
--
-- Nesting

-- two level nesting: VWAP
SELECT sum(B2.P*B2.V) FROM BIDS B2
    WHERE 0.25*(SELECT sum(BIDS.V) FROM BIDS) >
        (SELECT sum(B1.V) FROM BIDS B1 WHERE B1.P > B2.P);

-- nesting, comparing against constant
SELECT sum(B2.P*B2.V) FROM BIDS B2
    WHERE 5000 > (SELECT sum(B1.V) FROM BIDS B1 WHERE B1.P > B2.P);

-- nesting, comparing against attribute
SELECT sum(B2.P*B2.V) FROM BIDS B2
    WHERE B2.V > (SELECT sum(B1.V) FROM BIDS B1 WHERE B1.P > B2.P);


-- three level nesting, mixing comparing subqueries, attributes
-- Note: this currently doesn't compile correctly
SELECT sum(B2.P*B2.V) FROM BIDS B2
    WHERE 0.25*(SELECT sum(BIDS.V) FROM BIDS) >
        (SELECT sum(B1.V) FROM BIDS B1
         WHERE
             B1.V < (SELECT sum(B3.V) FROM BIDS B3 WHERE B1.P < B3.P)
             AND B1.P > B2.P);


--
--
-- Adaptor examples for executable engines

-- Orderbook schema and adaptors

-- Note different adaptors for bids and asks, the adaptors
-- split the exchange's message stream.
CREATE TABLE BIDS (t int, id int, broker_id int, p int, v int)
    FROM 'file'
    SOURCE 'DBToaster::DemoDatasets::OrderbookFileStream'
    ARGS '"20081201.csv",10000'
    INSTANCE 'VwapBids'
    TUPLE 'DBToaster::DemoDatasets::OrderbookTuple'
    ADAPTOR 'DBToaster::DemoDatasets::BidsOrderbookTupleAdaptor'
    BINDINGS 't,t,id,id,broker_id,broker_id,p,price,v,volume';

CREATE TABLE ASKS (t int, id int, broker_id int, p int, v int)
    FROM 'file'
    SOURCE 'DBToaster::DemoDatasets::OrderbookFileStream'
    ARGS '"20081201.csv",10000'
    INSTANCE 'VwapAsks'
    TUPLE 'DBToaster::DemoDatasets::OrderbookTuple'
    ADAPTOR 'DBToaster::DemoDatasets::AsksOrderbookTupleAdaptor'
    BINDINGS 't,t,id,id,broker_id,broker_id,p,price,v,volume';

/*
-- bindings are implicitly equivalent to fields if not specified
CREATE TABLE BIDS (t int, id int, broker_id int, price int, volume int)
    FROM 'file'
    SOURCE 'DBToaster::DemoDatasets::OrderbookFileStream'
    ARGS '"20081201.csv",10000'
    INSTANCE 'VwapBids'
    TUPLE 'DBToaster::DemoDatasets::BidsOrderbookTuple'
    ADAPTOR 'DBToaster::DemoDatasets::OrderbookTupleAdaptor';
*/

-- Full TPC-H schema and adaptors
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
        shippriority   integer,
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
    ARGS '"/Users/yanif/datasets/tpch/sf1/singlefile/nation.tbl",&DBToaster::DemoDatasets::parseNationField,4,100,512'
    INSTANCE 'SSBNation'
    TUPLE 'DBToaster::DemoDatasets::nation'
    ADAPTOR 'DBToaster::DemoDatasets::NationTupleAdaptor';


-- Note network sources are not really functional for now, and only work for
-- Anton's simple exchange server.