\timing
ALTER SEQUENCE pip_var_id RESTART WITH 1;

---------------------- Initialization -------------------------------

--CREATE TEMPORARY VIEW increase_per_part AS
--EXPLAIN
CREATE TEMPORARY TABLE increase_per_part AS
  SELECT
    expectation_sum((increase + 1) * "L_QUANTITY" * popularity, partpopularity) AS sales,
    "L_PARTKEY" AS part
  FROM
    "LINEITEM","ORDERS",
    (SELECT
      part,popularity
    FROM
      (SELECT
        "P_PARTKEY" AS part,
        CREATE_VARIABLE('Exponential', row(1)) AS popularity
      FROM
        "PART"
      WHERE
        "P_PARTKEY" <= 5000) AS pp
    WHERE
      popularity > 5.29) partpopularity,
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
    expectation_sum((increase + 1) * "L_QUANTITY" * popularity, partpopularity) AS sales,
    "L_PARTKEY" AS part
  FROM
    "LINEITEM","ORDERS",
    (SELECT
      part,popularity
    FROM
      (SELECT
        "P_PARTKEY" AS part,
        CREATE_VARIABLE('Exponential', row(1)) AS popularity
      FROM
        "PART"
      WHERE
        "P_PARTKEY" <= 5000) AS pp
    WHERE
      popularity > 4.6) partpopularity,
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
    expectation_sum((increase + 1) * "L_QUANTITY" * popularity, partpopularity) AS sales,
    "L_PARTKEY" AS part
  FROM
    "LINEITEM","ORDERS",
    (SELECT
      part,popularity
    FROM
      (SELECT
        "P_PARTKEY" AS part,
        CREATE_VARIABLE('Exponential', row(1)) AS popularity
      FROM
        "PART"
      WHERE
        "P_PARTKEY" <= 5000) AS pp
    WHERE
      popularity > 3) partpopularity,
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
    expectation_sum((increase + 1) * "L_QUANTITY" * popularity, partpopularity) AS sales,
    "L_PARTKEY" AS part
  FROM
    "LINEITEM","ORDERS",
    (SELECT
      part,popularity
    FROM
      (SELECT
        "P_PARTKEY" AS part,
        CREATE_VARIABLE('Exponential', row(1)) AS popularity
      FROM
        "PART"
      WHERE
        "P_PARTKEY" <= 5000) AS pp
    WHERE
      popularity > 1.4) partpopularity,
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