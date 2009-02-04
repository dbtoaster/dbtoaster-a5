\timing
ALTER SEQUENCE pip_var_id RESTART WITH 1;

---------------------- Initialization -------------------------------
-- A view of all parts purchaased from suppliers in japan
CREATE TEMPORARY VIEW from_japan AS
  SELECT *
  FROM "NATION", "SUPPLIER", "LINEITEM", "PARTSUPP"
  WHERE "N_NAME" = 'JAPAN'
    AND "S_SUPPKEY"   = "PS_SUPPKEY"
    AND "PS_PARTKEY"  = "L_PARTKEY"
    AND "PS_SUPPKEY"  = "L_SUPPKEY"
    AND "N_NATIONKEY" = "S_NATIONKEY";

-- For each customer, the number of orders placed in both 1996 and 1997
-- Return incr,custkey, where incr is the ratio of the orders for 1997 to those in 1996
CREATE TEMPORARY VIEW increase_per_customer AS
  SELECT
    newcount/oldcount AS incr, custkey
  FROM (
    SELECT 
      "O_CUSTKEY" AS custkey,
      SUM(extract(year from "O_ORDERDATE")-1996.0) AS newcount,
      SUM(1997.0-extract(year from "O_ORDERDATE")) AS oldcount
    FROM "ORDERS"
    WHERE extract(year from "O_ORDERDATE") = 1996.0 OR extract(year from "O_ORDERDATE") = 1997.0
    GROUP BY "O_CUSTKEY"
    ) AS counts
  WHERE
    oldcount > 0;

--------------------- Query computation --------------------------------
-- Create a variable representing the expected increase in purchasing for 1998
-- This is a Poisson distribution with a Lambda = Purchases(1997)/Purchases(1996)
-- (which is conveniently calculated by `increase_per_customer`)
CREATE TEMPORARY TABLE order_increase_mcdb AS
  SELECT
    "O_ORDERKEY",
    CREATE_VARIABLE('Poisson', increase_per_customer) AS new_cnt
  FROM
    "ORDERS",
    increase_per_customer
  WHERE "O_CUSTKEY" = custkey;

-- Generate a tuple bundle for each row, and compute the increased revenue.
-- Then after computing the expectation, compute the sum.
SELECT SUM(pip_value_bundle_expect(newRev)-oldRev)
FROM (
  SELECT 
    pip_value_bundle_mul(pip_value_bundle_create(new_cnt, 1000), "L_EXTENDEDPRICE" * (1.0 - "L_DISCOUNT")) AS newRev,
    "L_EXTENDEDPRICE" * (1.0 - "L_DISCOUNT") AS oldRev
  FROM order_increase_mcdb, from_japan
  WHERE "L_ORDERKEY" = "O_ORDERKEY"
  ) AS extendedPricing;
