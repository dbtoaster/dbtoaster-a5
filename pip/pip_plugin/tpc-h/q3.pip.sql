\timing
ALTER SEQUENCE pip_var_id RESTART WITH 1;

---------------------- Initialization -------------------------------

CREATE TEMPORARY VIEW orders_today AS
  SELECT * 
  FROM "ORDERS","LINEITEM" 
  WHERE "O_ORDERDATE"='1998-08-02' AND "O_ORDERKEY"="L_ORDERKEY";

CREATE OR REPLACE VIEW params AS
  SELECT 
    AVG   ("L_SHIPDATE"    - "O_ORDERDATE") AS ship_mu,
    AVG   ("L_RECEIPTDATE" - "L_SHIPDATE" ) AS arrv_mu,
    STDDEV("L_SHIPDATE"    - "O_ORDERDATE") AS ship_sigma,
    STDDEV("L_RECEIPTDATE" - "L_SHIPDATE" ) AS arrv_sigma,
    "L_PARTKEY" AS "P_PARTKEY"
  FROM "ORDERS","LINEITEM"
  WHERE "O_ORDERKEY" = "L_ORDERKEY"
  GROUP BY "L_PARTKEY";

--------------------- Query computation --------------------------------

CREATE TEMPORARY TABLE ship_duration AS 
  SELECT
    orders_today."O_ORDERKEY" AS orderkey,
    CREATE_VARIABLE('Normal', row(input_params.ship_mu, input_params.ship_sigma)) AS ship,
    CREATE_VARIABLE('Normal', row(input_params.arrv_mu, input_params.arrv_sigma)) AS arrv
  FROM
    orders_today,
    params AS input_params
  WHERE
    input_params."P_PARTKEY" = orders_today."L_PARTKEY";

CREATE TEMPORARY TABLE long_orders AS
  SELECT 
    orderkey,
    ship, arrv
  FROM ship_duration
  WHERE ship > 90
  UNION ALL  
  SELECT 
    orderkey,
    ship, arrv
  FROM ship_duration
  WHERE arrv > 60;

--------------------- Sampling --------------------------------

CREATE TEMPORARY TABLE final_results AS
  SELECT
    orderkey,
    conf_naive(long_orders, sampleset) AS dangerchance
  FROM 
    long_orders,
    (SELECT cast('?1000/0' AS pip_sample_set) AS sampleset) AS sampleset_input
  GROUP BY
    orderkey;

