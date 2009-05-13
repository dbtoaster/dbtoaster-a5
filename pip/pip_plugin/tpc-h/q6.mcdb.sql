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
      pip_value_bundle_create(CREATE_VARIABLE('Exponential', row(supply_dev*0.000001)), 60000),
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
        pip_value_bundle_create(CREATE_VARIABLE('Poisson', row(increase)), 60000),
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

CREATE TEMPORARY TABLE sample_count(cnt integer, delta integer);
INSERT INTO sample_count(cnt,delta) VALUES 
  (1,0)       ,
  (2,30)      ,
  (5,90)      ,
  (10,240)    ,
  (25,540)    ,
  (50,1390)   ,
  (100,2790)  ,
  (250,5790)  ,
  (500,13290) ,
  (1000,28290);

CREATE TEMPORARY TABLE sample_instance(num integer);
INSERT INTO sample_instance(num) VALUES 
  (00), (01), (02), (03), (04), (05), (06), (07), (08), (09),
  (10), (11), (12), (13), (14), (15), (16), (17), (18), (19),
  (20), (21), (22), (23), (24), (25), (26), (27), (28), (29);

CREATE TEMPORARY TABLE samplepoints AS
  SELECT 
    num, cnt, cnt*num+delta AS low, cnt*(num+1)+delta AS high
  FROM 
    sample_count, sample_instance;

CREATE TEMPORARY TABLE results AS
  SELECT
    part,cnt,pip_world_presence_count(worldpresence) AS reliability,
    pip_value_bundle_expect(deficit, low, high, worldpresence) AS underproduction
  FROM
    (SELECT
      pip_value_bundle_cmp(pip_world_presence_create(60000), customer_demand, supply) AS worldpresence,
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
--  GROUP BY 
--    demand_tbl.part;

SELECT cnt, avg(sqrt(underproduction)), avg(reliability), count(*) AS reliability
FROM   (
          SELECT    cnt, results.part, avg((results.underproduction-correct_results.underproduction)*(results.underproduction-correct_results.underproduction)/(correct_results.underproduction*correct_results.underproduction)) as underproduction, avg(reliability) AS reliability
          FROM      results, correct_results
          WHERE     results.part = correct_results.part AND results.underproduction != 'Infinity' AND correct_results.underproduction != 'Infinity'
          GROUP BY  cnt, results.part
       ) resultdevs
GROUP BY cnt;