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


open Calculus
open Util;;

let cg = Compiler.generate_unit_test_code;;

let sch =
[
("SSB_DATE", [("datekey", TInt); ("year", TInt)]);
("SSB_CUSTOMER", [("custkey", TInt); ("name", TInt); ("address", TInt); ("city", TInt);
        ("nation", TInt); ("region", TInt); ("phone", TInt); ("mktsegment", TInt)]);
("SSB_SUPPLIER", [("suppkey", TInt); ("name", TInt); ("address", TInt); ("city", TInt);
        ("nation", TInt); ("region", TInt); ("phone", TInt)]);
("SSB_PART", [("partkey", TInt); ("name", TInt); ("mfgr", TInt)]);
("SSB_LINEORDER", [("orderkey", TInt); ("linenumber", TInt);
        ("custkey", TInt); ("partkey", TInt); ("suppkey", TInt);
        ("orderpriority", TInt); ("shippriority", TInt);
        ("quantity", TInt); ("revenue", TInt); ("orderdate", TInt); ("supplycost", TInt)])
];;


let ssb =
  RVal
    (AggSum
      (RSum
        [RVal (Var ("LO__REVENUE", TInt));
         RProd
          [RVal (Const (Int (-1)));
           RVal (Var ("LO__SUPPLYCOST", TInt))]],
       RA_MultiNatJoin
        [RA_MultiNatJoin
          [RA_Leaf
            (Rel ("SSB_DATE",
              [("D__DATEKEY", TInt); ("D__YEAR", TInt)]));
           RA_Leaf
            (Rel ("SSB_CUSTOMER",
              [("C__CUSTKEY", TInt); ("C__NAME", TInt);
               ("C__ADDRESS", TInt); ("C__CITY", TInt);
               ("C__NATION", TInt);
               ("C__REGION", TInt); ("C__PHONE", TInt);
               ("C__MKTSEGMENT", TInt)]));
           RA_Leaf
            (Rel ("SSB_SUPPLIER",
              [("S__SUPPKEY", TInt); ("S__NAME", TInt);
               ("S__ADDRESS", TInt); ("S__CITY", TInt);
               ("S__NATION", TInt);
               ("S__REGION", TInt); ("S__PHONE", TInt)]));
           RA_Leaf
            (Rel ("SSB_PART",
              [("P__PARTKEY", TInt); ("P__NAME", TInt);
               ("P__MFGR", TInt)]));
           RA_Leaf
            (Rel ("SSB_LINEORDER",
              [("LO__ORDERKEY", TInt);
               ("LO__LINENUMBER", TInt);
               ("LO__CUSTKEY", TInt);
               ("LO__PARTKEY", TInt);
               ("LO__SUPPKEY", TInt);
               ("LO__ORDERPRIORITY", TInt);
               ("LO__SHIPPRIORITY", TInt);
               ("LO__QUANTITY", TInt);
               ("LO__REVENUE", TInt);
               ("LO__ORDERDATE", TInt);
               ("LO__SUPPLYCOST", TInt)]))];
         RA_MultiNatJoin
          [RA_Leaf
            (AtomicConstraint (Eq,
              RVal (Var ("LO__CUSTKEY", TInt)),
              RVal (Var ("C__CUSTKEY", TInt))));
           RA_MultiNatJoin
            [RA_Leaf
              (AtomicConstraint (Eq,
                RVal (Var ("LO__SUPPKEY", TInt)),
                RVal (Var ("S__SUPPKEY", TInt))));
             RA_MultiNatJoin
              [RA_Leaf
                (AtomicConstraint (Eq,
                  RVal (Var ("LO__PARTKEY", TInt)),
                  RVal (Var ("P__PARTKEY", TInt))));
               RA_Leaf
                (AtomicConstraint (Eq,
                  RVal (Var ("LO__ORDERDATE", TInt)),
                  RVal (Var ("D__DATEKEY", TInt))))]]]]))
;;

(* This is a large piece of code...*)
Compiler.compile Calculus.ModeExtractFromCond
                 sch (make_term ssb, (map_term "m" [("D", TInt)])) cg []
;;



