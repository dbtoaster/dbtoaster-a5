\timing
ALTER SEQUENCE pip_var_id RESTART WITH 1;

---------------------- Initialization -------------------------------

CREATE TEMPORARY VIEW production_history AS
  SELECT 
    "L_PARTKEY" AS part, 
    "L_SUPPKEY" AS supplier,
    extract(year from "O_ORDERDATE") AS year, 
    sum("L_QUANTITY") AS count 
  FROM 
    "LINEITEM", 
    "ORDERS" 
  WHERE 
    "L_PARTKEY" <= 5000
    AND "O_ORDERKEY" = "L_ORDERKEY" 
    AND extract (year from "O_ORDERDATE") < 1998
    AND extract (year from "O_ORDERDATE") >= 1992
  GROUP BY 
    part, supplier, year;

CREATE TEMPORARY VIEW production_stats AS
  SELECT   part, max(count) as supply_max, stddev(count) as supply_dev
  FROM     production_history
  GROUP BY part;

CREATE TEMPORARY TABLE supply_tbl AS
  SELECT
    part,  
    pip_value_bundle_add(
      pip_value_bundle_create(CREATE_VARIABLE('Exponential', row(supply_dev*0.000001)), 200000),
      supply_max
    ) AS supply
  FROM
    production_stats;

CREATE TEMPORARY VIEW increase_per_customer AS
  SELECT
    newcount/oldcount AS increase, customer
  FROM (
    SELECT 
      "O_CUSTKEY" AS customer,
      SUM(extract(year from "O_ORDERDATE")-1996.0) AS newcount,
      SUM(1997.0-extract(year from "O_ORDERDATE")) AS oldcount
    FROM "ORDERS"
    WHERE extract(year from "O_ORDERDATE") = 1996.0 OR extract(year from "O_ORDERDATE") = 1997.0
    GROUP BY customer
    ) AS counts
  WHERE
    oldcount > 0;

CREATE TEMPORARY TABLE demand_tbl AS
  SELECT
    part, price,
    pip_value_bundle_mul(
      pip_value_bundle_add(
        pip_value_bundle_create(CREATE_VARIABLE('Poisson', row(increase)), 200000),
        1
      ),
      quantity
    ) AS customer_demand
  FROM
    (SELECT
      "L_PARTKEY" AS part,
      sum(("L_EXTENDEDPRICE" * "L_QUANTITY" * (1-"L_DISCOUNT"))) AS price,
      sum("L_QUANTITY") AS quantity,
      avg(increase) AS increase
    FROM
      "ORDERS",
      increase_per_customer,
      "LINEITEM"
    WHERE
      "L_PARTKEY" <= 5000 
      AND "O_CUSTKEY" = customer
      AND "O_ORDERKEY" = "L_ORDERKEY"
    GROUP BY
      part
    ) AS average_increase;

CREATE TEMPORARY TABLE results AS
  SELECT
    part,cnt,pip_world_presence_count(worldpresence) AS reliability,
    pip_value_bundle_expect(deficit, worldpresence) AS underproduction
  FROM
    (SELECT
      pip_value_bundle_cmp(pip_world_presence_create(200000), customer_demand, supply) AS worldpresence,
      demand_tbl.part,
      pip_value_bundle_add(
        customer_demand,
        pip_value_bundle_mul(
          supply,
          -1
        )
      ) AS deficit
    FROM
      demand_tbl,
      supply_tbl
    WHERE
      demand_tbl.part = supply_tbl.part
    ) AS inputs,
    samplepoints;
    