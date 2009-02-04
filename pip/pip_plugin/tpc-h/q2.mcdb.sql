\timing
ALTER SEQUENCE pip_var_id RESTART WITH 1;

---------------------- Initialization -------------------------------
CREATE TEMPORARY VIEW orders_today AS
  SELECT * 
  FROM "ORDERS","LINEITEM" 
  WHERE "O_ORDERDATE"='1998-08-02' AND "O_ORDERKEY"="L_ORDERKEY";

CREATE TEMPORARY VIEW params AS
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
SELECT
  expectation_max(duration)
FROM
  (SELECT
    pip_value_bundle_add(pip_value_bundle_create(ship, 1000), pip_value_bundle_create(arrv, 1000)) AS duration
  FROM
    (SELECT
      CREATE_VARIABLE('Normal', row(input_params.ship_mu, input_params.ship_sigma)) AS ship,
      CREATE_VARIABLE('Normal', row(input_params.arrv_mu, input_params.arrv_sigma)) AS arrv
    FROM
      orders_today,
      params AS input_params
    WHERE
      input_params."P_PARTKEY" = orders_today."L_PARTKEY"
    ) AS separate_durations
  ) AS ship_durations;
