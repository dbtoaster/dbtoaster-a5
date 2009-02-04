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
CREATE TEMPORARY TABLE order_increase AS
  SELECT
    "O_ORDERKEY",
    CREATE_VARIABLE('Poisson', increase_per_customer) AS new_cnt
  FROM
    "ORDERS",
    increase_per_customer
  WHERE "O_CUSTKEY" = custkey;

-- Given the % order increase we just created variables for, compute the increase
-- in purchases made for parts ordered from japan.  TPCH has a per-order "discount"
-- field, so we need to do a little tomfoolery to compute the actual price a 
-- customer is expected to pay for a particular part.
CREATE TEMPORARY TABLE extended_pricing AS
  SELECT 
    ((new_cnt + 1) * ("L_EXTENDEDPRICE" * (1.0 - "L_DISCOUNT"))) + (0.0-("L_EXTENDEDPRICE" * (1.0 - "L_DISCOUNT"))) AS newRev
  FROM order_increase, from_japan
  WHERE "L_ORDERKEY" = "O_ORDERKEY";


--------------------- Sampling --------------------------------
-- Compute a histogram of pricing ranges.
CREATE TEMPORARY TABLE unexploded_results AS
  SELECT expectation_sum_hist(newRev,extended_pricing)
  FROM extended_pricing;

-- Because of the way aggregates and set returning functions interact... we need 
-- to do a little bit of a dance to get the individual values.  These functions 
-- can probably be merged into an SQL function; but for now, it's not a priority.
CREATE TEMPORARY TABLE final_results AS 
  SELECT * FROM pip_sample_set_explode ((SELECT * FROM unexploded_results));

SELECT * FROM final_results;

--------------------- Alternate Sampling --------------------------------
-- Alternatively, we can compute just the sum of the individual expectations.
-- Mostly, this is just a test for the angle brackets hack.
SELECT sum( << newRev >> ) FROM extended_pricing;
