(*
create table SSB_DATE (datekey bigint, year text);

create table SSB_CUSTOMER (
        custkey bigint, name text, address text, city text,
        nation text, region text, phone text, mktsegment text
        );

create table SSB_SUPPLIER (
        suppkey bigint, name text, address text, city text,
        nation text, region text, phone text
        );

create table SSB_PART (
        partkey bigint, name text, mfgr text
        );

create table SSB_LINEORDER (
        orderkey bigint, linenumber integer,
        custkey bigint, partkey bigint, suppkey bigint,
        orderpriority text, shippriority integer,
        quantity double, revenue double, orderdate int, supplycost int
        );

select d.year, c.nation, c.region, s.region, p.mfgr,
       sum(lo.revenue + ((-1) * lo.supplycost))
from
    ssb_date d, ssb_customer c, ssb_supplier s, ssb_part p, ssb_lineorder lo
where lo.custkey = c.custkey
and lo.suppkey = s.suppkey
and lo.partkey = p.partkey
and lo.orderdate = d.datekey
group by d.year, c.nation, c.region, s.region, p.mfgr;
*)


open Calculus;;

let sch =
[
("SSB_DATE", ["datekey"; "year"]);
("SSB_CUSTOMER", ["custkey"; "name"; "address"; "city";
        "nation"; "region"; "phone"; "mktsegment"]);
("SSB_SUPPLIER", ["suppkey"; "name"; "address"; "city";
        "nation"; "region"; "phone"]);
("SSB_PART", ["partkey"; "name"; "mfgr"]);
("SSB_LINEORDER", ["orderkey"; "linenumber";
        "custkey"; "partkey"; "suppkey";
        "orderpriority"; "shippriority";
        "quantity"; "revenue"; "orderdate"; "supplycost"])
];;


let ssb =
  RVal
    (AggSum
      (RSum
        [RVal (Var "LO__REVENUE");
         RProd
          [RVal (Const (Int (-1)));
           RVal (Var "LO__SUPPLYCOST")]],
       RA_MultiNatJoin
        [RA_MultiNatJoin
          [RA_Leaf
            (Rel ("SSB_DATE",
              ["D__DATEKEY"; "D__YEAR"]));
           RA_Leaf
            (Rel ("SSB_CUSTOMER",
              ["C__CUSTKEY"; "C__NAME";
               "C__ADDRESS"; "C__CITY";
               "C__NATION";
               "C__REGION"; "C__PHONE";
               "C__MKTSEGMENT"]));
           RA_Leaf
            (Rel ("SSB_SUPPLIER",
              ["S__SUPPKEY"; "S__NAME";
               "S__ADDRESS"; "S__CITY";
               "S__NATION";
               "S__REGION"; "S__PHONE"]));
           RA_Leaf
            (Rel ("SSB_PART",
              ["P__PARTKEY"; "P__NAME";
               "P__MFGR"]));
           RA_Leaf
            (Rel ("SSB_LINEORDER",
              ["LO__ORDERKEY";
               "LO__LINENUMBER";
               "LO__CUSTKEY";
               "LO__PARTKEY";
               "LO__SUPPKEY";
               "LO__ORDERPRIORITY";
               "LO__SHIPPRIORITY";
               "LO__QUANTITY";
               "LO__REVENUE";
               "LO__ORDERDATE";
               "LO__SUPPLYCOST"]))];
         RA_MultiNatJoin
          [RA_Leaf
            (AtomicConstraint (Eq,
              RVal (Var "LO__CUSTKEY"),
              RVal (Var "C__CUSTKEY")));
           RA_MultiNatJoin
            [RA_Leaf
              (AtomicConstraint (Eq,
                RVal (Var "LO__SUPPKEY"),
                RVal (Var "S__SUPPKEY")));
             RA_MultiNatJoin
              [RA_Leaf
                (AtomicConstraint (Eq,
                  RVal (Var "LO__PARTKEY"),
                  RVal (Var "P__PARTKEY")));
               RA_Leaf
                (AtomicConstraint (Eq,
                  RVal (Var "LO__ORDERDATE"),
                  RVal (Var "D__DATEKEY")))]]]]))
;;


Compiler.compile Calculus.ModeExtractFromCond
                 sch (Compiler.mk_external "m" ["D"])
                 (make_term ssb)
;;



