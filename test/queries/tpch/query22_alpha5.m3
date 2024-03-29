-------------------- SOURCES --------------------
CREATE STREAM ORDERS(ORDERS_ORDERKEY int, ORDERS_CUSTKEY int, ORDERS_ORDERSTATUS string, ORDERS_TOTALPRICE float, ORDERS_ORDERDATE date, ORDERS_ORDERPRIORITY string, ORDERS_CLERK string, ORDERS_SHIPPRIORITY int, ORDERS_COMMENT string)
  FROM FILE '../dbtoaster-experiments-data/tpch/big/orders.csv' LINE DELIMITED
  CSV(delimiter := '|');

CREATE STREAM CUSTOMER(CUSTOMER_CUSTKEY int, CUSTOMER_NAME string, CUSTOMER_ADDRESS string, CUSTOMER_NATIONKEY int, CUSTOMER_PHONE string, CUSTOMER_ACCTBAL float, CUSTOMER_MKTSEGMENT string, CUSTOMER_COMMENT string)
  FROM FILE '../dbtoaster-experiments-data/tpch/big/customer.csv' LINE DELIMITED
  CSV(delimiter := '|');

--------------------- MAPS ----------------------
DECLARE MAP QUERY22[][C1_NATIONKEY:int] := 
AggSum([C1_NATIONKEY], 
  (CUSTOMER(C1_CUSTKEY, C1_NAME, C1_ADDRESS, C1_NATIONKEY, C1_PHONE,
              C1_ACCTBAL, C1_MKTSEGMENT, C1_COMMENT) *
    AggSum([], 
      ((__sql_inline_agg_2 ^=
         AggSum([], 
           (CUSTOMER(C2_CUSTKEY, C2_NAME, C2_ADDRESS, C2_NATIONKEY, C2_PHONE,
                       C2_ACCTBAL, C2_MKTSEGMENT, C2_COMMENT) *
             {C2_ACCTBAL > 0} * C2_ACCTBAL))) *
        {C1_ACCTBAL < __sql_inline_agg_2})) *
    AggSum([C1_CUSTKEY], 
      ((__sql_inline_agg_1 ^=
         AggSum([C1_CUSTKEY], 
           ORDERS(O_ORDERKEY, C1_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE,
                    O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY,
                    O_COMMENT))) *
        (__sql_inline_agg_1 ^= 0))) *
    C1_ACCTBAL));

DECLARE MAP QUERY22_mCUSTOMER1[][C1_CUSTKEY:int, C1_NATIONKEY:int, C1_ACCTBAL:float] := 
AggSum([C1_CUSTKEY, C1_NATIONKEY, C1_ACCTBAL], 
  (CUSTOMER(C1_CUSTKEY, C1_NAME, C1_ADDRESS, C1_NATIONKEY, C1_PHONE,
              C1_ACCTBAL, C1_MKTSEGMENT, C1_COMMENT) *
    C1_ACCTBAL));

DECLARE MAP QUERY22_mCUSTOMER1_L2_1[][] := 
AggSum([], 
  (CUSTOMER(C2_CUSTKEY, C2_NAME, C2_ADDRESS, C2_NATIONKEY, C2_PHONE,
              C2_ACCTBAL, C2_MKTSEGMENT, C2_COMMENT) *
    {C2_ACCTBAL > 0} * C2_ACCTBAL));

DECLARE MAP QUERY22_mCUSTOMER1_L3_1(int)[][C1_CUSTKEY:int] := 
AggSum([C1_CUSTKEY], 
  ORDERS(O_ORDERKEY, C1_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE,
           O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT));

-------------------- QUERIES --------------------
DECLARE QUERY QUERY22 := QUERY22(float)[][C1_NATIONKEY];

------------------- TRIGGERS --------------------
ON + ORDERS(ORDERS_ORDERKEY, ORDERS_CUSTKEY, ORDERS_ORDERSTATUS, ORDERS_TOTALPRICE, ORDERS_ORDERDATE, ORDERS_ORDERPRIORITY, ORDERS_CLERK, ORDERS_SHIPPRIORITY, ORDERS_COMMENT) {
   QUERY22(float)[][C1_NATIONKEY] += 
  (AggSum([ORDERS_CUSTKEY, C1_NATIONKEY], 
   ((__sql_inline_agg_2 ^= QUERY22_mCUSTOMER1_L2_1[][]) *
     QUERY22_mCUSTOMER1[][ORDERS_CUSTKEY, C1_NATIONKEY, C1_ACCTBAL] *
     {C1_ACCTBAL < __sql_inline_agg_2})) *
  (AggSum([], 
     ((__sql_inline_agg_1 ^= 0) *
       (__sql_inline_agg_1 ^=
         (QUERY22_mCUSTOMER1_L3_1(int)[][ORDERS_CUSTKEY] + 1)))) +
    (AggSum([ORDERS_CUSTKEY], 
       ((__sql_inline_agg_1 ^= 0) *
         (__sql_inline_agg_1 ^=
           QUERY22_mCUSTOMER1_L3_1(int)[][ORDERS_CUSTKEY]))) *
      -1)));
   QUERY22_mCUSTOMER1_L3_1(int)[][ORDERS_CUSTKEY] += 1;
}

ON - ORDERS(ORDERS_ORDERKEY, ORDERS_CUSTKEY, ORDERS_ORDERSTATUS, ORDERS_TOTALPRICE, ORDERS_ORDERDATE, ORDERS_ORDERPRIORITY, ORDERS_CLERK, ORDERS_SHIPPRIORITY, ORDERS_COMMENT) {
   QUERY22(float)[][C1_NATIONKEY] += 
  (AggSum([C1_NATIONKEY], 
   ((__sql_inline_agg_2 ^= QUERY22_mCUSTOMER1_L2_1[][]) *
     (__sql_inline_agg_1 ^= 0) *
     QUERY22_mCUSTOMER1[][C1_CUSTKEY, C1_NATIONKEY, C1_ACCTBAL] *
     (__sql_inline_agg_1 ^=
       (QUERY22_mCUSTOMER1_L3_1(int)[][C1_CUSTKEY] +
         ((C1_CUSTKEY ^= ORDERS_CUSTKEY) * -1))) *
     {C1_ACCTBAL < __sql_inline_agg_2})) +
  (AggSum([C1_NATIONKEY], 
     ((__sql_inline_agg_2 ^= QUERY22_mCUSTOMER1_L2_1[][]) *
       (__sql_inline_agg_1 ^= 0) *
       QUERY22_mCUSTOMER1[][C1_CUSTKEY, C1_NATIONKEY, C1_ACCTBAL] *
       (__sql_inline_agg_1 ^= QUERY22_mCUSTOMER1_L3_1(int)[][C1_CUSTKEY]) *
       {C1_ACCTBAL < __sql_inline_agg_2})) *
    -1));
   QUERY22_mCUSTOMER1_L3_1(int)[][ORDERS_CUSTKEY] += -1;
}

ON + CUSTOMER(CUSTOMER_CUSTKEY, CUSTOMER_NAME, CUSTOMER_ADDRESS, CUSTOMER_NATIONKEY, CUSTOMER_PHONE, CUSTOMER_ACCTBAL, CUSTOMER_MKTSEGMENT, CUSTOMER_COMMENT) {
   QUERY22_mCUSTOMER1(float)[][CUSTOMER_CUSTKEY, CUSTOMER_NATIONKEY, CUSTOMER_ACCTBAL] += CUSTOMER_ACCTBAL;
   QUERY22_mCUSTOMER1_L2_1(float)[][] += ({CUSTOMER_ACCTBAL > 0} * CUSTOMER_ACCTBAL);
   QUERY22(float)[][C1_NATIONKEY] := 
  AggSum([C1_NATIONKEY], 
  ((__sql_inline_agg_2 ^= QUERY22_mCUSTOMER1_L2_1[][]) *
    (__sql_inline_agg_1 ^= 0) *
    QUERY22_mCUSTOMER1[][C1_CUSTKEY, C1_NATIONKEY, C1_ACCTBAL] *
    (__sql_inline_agg_1 ^= QUERY22_mCUSTOMER1_L3_1(int)[][C1_CUSTKEY]) *
    {C1_ACCTBAL < __sql_inline_agg_2}));
}

ON - CUSTOMER(CUSTOMER_CUSTKEY, CUSTOMER_NAME, CUSTOMER_ADDRESS, CUSTOMER_NATIONKEY, CUSTOMER_PHONE, CUSTOMER_ACCTBAL, CUSTOMER_MKTSEGMENT, CUSTOMER_COMMENT) {
   QUERY22_mCUSTOMER1(float)[][CUSTOMER_CUSTKEY, CUSTOMER_NATIONKEY, CUSTOMER_ACCTBAL] += (-1 * CUSTOMER_ACCTBAL);
   QUERY22_mCUSTOMER1_L2_1(float)[][] += ({CUSTOMER_ACCTBAL > 0} * -1 * CUSTOMER_ACCTBAL);
   QUERY22(float)[][C1_NATIONKEY] := 
  AggSum([C1_NATIONKEY], 
  ((__sql_inline_agg_2 ^= QUERY22_mCUSTOMER1_L2_1[][]) *
    (__sql_inline_agg_1 ^= 0) *
    QUERY22_mCUSTOMER1[][C1_CUSTKEY, C1_NATIONKEY, C1_ACCTBAL] *
    (__sql_inline_agg_1 ^= QUERY22_mCUSTOMER1_L3_1(int)[][C1_CUSTKEY]) *
    {C1_ACCTBAL < __sql_inline_agg_2}));
}

ON SYSTEM READY {
}
