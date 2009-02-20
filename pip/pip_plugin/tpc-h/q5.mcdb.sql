\timing
ALTER SEQUENCE pip_var_id RESTART WITH 1;

---------------------- Initialization -------------------------------

--CREATE TEMPORARY VIEW increase_per_part AS
--EXPLAIN
--CREATE TEMPORARY TABLE increase_per_part AS
--  SELECT
--    "L_PARTKEY" AS part,
--    sum(
--      pip_value_bundle_expect(
--        pip_value_bundle_mul(
--          pip_value_bundle_mul(
--            pip_value_bundle_add(
--              pip_value_bundle_create(increase, 200000), 
--              1.0),
--            "L_QUANTITY"),
--          popularity),
--        worldpresence)
--      ) AS sales
--  FROM
--    "LINEITEM","ORDERS",
--    (SELECT
--      part,popularity,
--      pip_value_bundle_cmp(pip_world_presence_create(200000), popularity, 5.29) AS worldpresence
--    FROM
--      (SELECT
--        "P_PARTKEY" AS part,
--        pip_value_bundle_create(CREATE_VARIABLE('Exponential', row(1)), 200000) AS popularity
--      FROM
--        "PART"
--      WHERE
--        "P_PARTKEY" <= 5000) AS partpopularity
--    ) partpopularity,
--    (SELECT
--      CREATE_VARIABLE('Poisson', row(newcount/oldcount)) AS increase, customer
--    FROM (
--      SELECT 
--        "O_CUSTKEY" AS customer,
--        SUM(extract(year from "O_ORDERDATE")-1996.0) AS newcount,
--        SUM(1997.0-extract(year from "O_ORDERDATE")) AS oldcount
--      FROM "ORDERS"
--      WHERE extract(year from "O_ORDERDATE") = 1996.0 OR extract(year from "O_ORDERDATE") = 1997.0 AND "O_CUSTKEY" <= 5000
--      GROUP BY customer
--      ) AS counts
--    WHERE
--      oldcount > 0) increases
--  WHERE
--    "O_CUSTKEY"=customer AND "O_ORDERKEY" = "L_ORDERKEY" AND "L_PARTKEY"=partpopularity.part
--  GROUP BY
--    "L_PARTKEY";

DROP TABLE IF EXISTS increase_per_part;

CREATE TEMPORARY TABLE increase_per_part AS
  SELECT
    "L_PARTKEY" AS part,
    sum(
      pip_value_bundle_expect(
        pip_value_bundle_mul(
          pip_value_bundle_mul(
            pip_value_bundle_add(
              pip_value_bundle_create(increase, 100000), 
              1.0),
            "L_QUANTITY"),
          popularity),
        worldpresence)
      ) AS sales
  FROM
    "LINEITEM","ORDERS",
    (SELECT
      part,popularity,
      pip_value_bundle_cmp(pip_world_presence_create(100000), popularity, 4.6) AS worldpresence
    FROM
      (SELECT
        "P_PARTKEY" AS part,
        pip_value_bundle_create(CREATE_VARIABLE('Exponential', row(1)), 100000) AS popularity
      FROM
        "PART"
      WHERE
        "P_PARTKEY" <= 5000) AS partpopularity
    ) partpopularity,
    (SELECT
      CREATE_VARIABLE('Poisson', row(newcount/oldcount)) AS increase, customer
    FROM (
      SELECT 
        "O_CUSTKEY" AS customer,
        SUM(extract(year from "O_ORDERDATE")-1996.0) AS newcount,
        SUM(1997.0-extract(year from "O_ORDERDATE")) AS oldcount
      FROM "ORDERS"
      WHERE extract(year from "O_ORDERDATE") = 1996.0 OR extract(year from "O_ORDERDATE") = 1997.0 AND "O_CUSTKEY" <= 5000
      GROUP BY customer
      ) AS counts
    WHERE
      oldcount > 0) increases
  WHERE
    "O_CUSTKEY"=customer AND "O_ORDERKEY" = "L_ORDERKEY" AND "L_PARTKEY"=partpopularity.part
  GROUP BY
    "L_PARTKEY";
DROP TABLE IF EXISTS increase_per_part;


CREATE TEMPORARY TABLE increase_per_part AS
  SELECT
    "L_PARTKEY" AS part,
    sum(
      pip_value_bundle_expect(
        pip_value_bundle_mul(
          pip_value_bundle_mul(
            pip_value_bundle_add(
              pip_value_bundle_create(increase, 20000), 
              1.0),
            "L_QUANTITY"),
          popularity),
        worldpresence)
      ) AS sales
  FROM
    "LINEITEM","ORDERS",
    (SELECT
      part,popularity,
      pip_value_bundle_cmp(pip_world_presence_create(20000), popularity, 3) AS worldpresence
    FROM
      (SELECT
        "P_PARTKEY" AS part,
        pip_value_bundle_create(CREATE_VARIABLE('Exponential', row(1)), 20000) AS popularity
      FROM
        "PART"
      WHERE
        "P_PARTKEY" <= 5000) AS partpopularity
    ) partpopularity,
    (SELECT
      CREATE_VARIABLE('Poisson', row(newcount/oldcount)) AS increase, customer
    FROM (
      SELECT 
        "O_CUSTKEY" AS customer,
        SUM(extract(year from "O_ORDERDATE")-1996.0) AS newcount,
        SUM(1997.0-extract(year from "O_ORDERDATE")) AS oldcount
      FROM "ORDERS"
      WHERE extract(year from "O_ORDERDATE") = 1996.0 OR extract(year from "O_ORDERDATE") = 1997.0 AND "O_CUSTKEY" <= 5000
      GROUP BY customer
      ) AS counts
    WHERE
      oldcount > 0) increases
  WHERE
    "O_CUSTKEY"=customer AND "O_ORDERKEY" = "L_ORDERKEY" AND "L_PARTKEY"=partpopularity.part
  GROUP BY
    "L_PARTKEY";
DROP TABLE IF EXISTS increase_per_part;


CREATE TEMPORARY TABLE increase_per_part AS
  SELECT
    "L_PARTKEY" AS part,
    sum(
      pip_value_bundle_expect(
        pip_value_bundle_mul(
          pip_value_bundle_mul(
            pip_value_bundle_add(
              pip_value_bundle_create(increase, 4000), 
              1.0),
            "L_QUANTITY"),
          popularity),
        worldpresence)
      ) AS sales
  FROM
    "LINEITEM","ORDERS",
    (SELECT
      part,popularity,
      pip_value_bundle_cmp(pip_world_presence_create(4000), popularity, 1.4) AS worldpresence
    FROM
      (SELECT
        "P_PARTKEY" AS part,
        pip_value_bundle_create(CREATE_VARIABLE('Exponential', row(1)), 4000) AS popularity
      FROM
        "PART"
      WHERE
        "P_PARTKEY" <= 5000) AS partpopularity
    ) partpopularity,
    (SELECT
      CREATE_VARIABLE('Poisson', row(newcount/oldcount)) AS increase, customer
    FROM (
      SELECT 
        "O_CUSTKEY" AS customer,
        SUM(extract(year from "O_ORDERDATE")-1996.0) AS newcount,
        SUM(1997.0-extract(year from "O_ORDERDATE")) AS oldcount
      FROM "ORDERS"
      WHERE extract(year from "O_ORDERDATE") = 1996.0 OR extract(year from "O_ORDERDATE") = 1997.0 AND "O_CUSTKEY" <= 5000
      GROUP BY customer
      ) AS counts
    WHERE
      oldcount > 0) increases
  WHERE
    "O_CUSTKEY"=customer AND "O_ORDERKEY" = "L_ORDERKEY" AND "L_PARTKEY"=partpopularity.part
  GROUP BY
    "L_PARTKEY";
DROP TABLE IF EXISTS increase_per_part;
