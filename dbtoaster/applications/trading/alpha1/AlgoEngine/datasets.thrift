//
// Orderbook data
struct VwapTuple
{
    1: i32 t,
    2: i32 id,
    3: i32 price,
    4: i32 volume
}


//
// SSB data

// SSB typedefs
typedef i64 identifier
typedef string date

struct Lineitem {
    1:  identifier orderkey,
    2:  identifier partkey,
    3:  identifier suppkey,
    4:  i32 linenumber,
    5:  double quantity,
    6:  double extendedprice,
    7:  double discount,
    8:  double tax,
    9:  string returnflag,
    10: string linestatus,
    11: date shipdate,
    12: date commitdate,
    13: date receiptdate,
    14: string shipinstruct,
    15: string shipmode,
    16: string comment
}

struct Order {
    1: identifier orderkey,
    2: identifier custkey,
    3: string orderstatus,
    4: double totalprice,
    5: date orderdate,
    6: string orderpriority,
    7: string clerk,
    8: i32 shippriority,
    9: string comment
}

struct Part {
    1: identifier partkey,
    2: string name,
    3: string mfgr,
    4: string brand,
    5: string type,
    6: i32 size,
    7: string container,
    8: double retailprice,
    9: string comment
}

struct Customer {
    1: identifier custkey,
    2: string name,
    3: string address,
    4: identifier nationkey,
    5: string phone,
    6: double acctbal,
    7: string mktsegment,
    8: string comment
}

struct Supplier {
    1: identifier suppkey,
    2: string name,
    3: string address,
    4: identifier nationkey,
    5: string phone,
    6: double acctbal,
    7: string comment
}

struct Nation {
    1: identifier nationkey,
    2: string name,
    3: identifier regionkey,
    4: string comment,
}

struct Region {
    1: identifier regionkey,
    2: string name,
    3: string comment,
}


struct SsbTuple
{
    1: i32 type,
    2: optional Lineitem li,
    3: optional Order ord,
    4: optional Part pt,
    5: optional Customer cust,
    6: optional Supplier supp,
    7: optional Nation nt,
    8: optional Region rg
}


//
// Linear Road data
struct LinearRoadTuple
{
    1:  i32 rectype,
    2:  i32 t,
    3:  i32 vid,
    4:  i32 speed,
    5:  i32 xway,
    6:  i32 lane,
    7:  i32 dir,
    8:  i32 seg,
    9:  i32 pos,
    10: i32 qid,
    11: i32 sinit,
    12: i32 send,
    13: i32 dow,
    14: i32 tod,
    15: i32 day
}
