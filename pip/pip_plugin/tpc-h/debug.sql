--ALTER SEQUENCE pip_var_id RESTART WITH 1;

--DROP TABLE IF EXISTS increase_per_part;
--CREATE TABLE increase_per_part AS
--  SELECT
--    popularity,
--    pip_value_bundle_mul(
--      pip_value_bundle_add(
--        pip_value_bundle_create(increase, 60000), 
--        1.0),
--      "L_QUANTITY") as mcdbincrease,
--    pip_value_bundle_mul(
--      pip_value_bundle_mul(
--        pip_value_bundle_add(
--          pip_value_bundle_create(increase, 60000), 
--          1.0),
--        "L_QUANTITY"),
--      mcdbpopularity) as mcdbsales,
--    pip_value_bundle_cmp(pip_world_presence_create(60000), mcdbpopularity, 5.29) AS mcdbpopularity,
--    (potincrease + 1) * "L_QUANTITY" as realincrease,
--    (potincrease + 1) * "L_QUANTITY" * 6.34 as realsales,
--    (increase + 1) * "L_QUANTITY" as pipincrease,
--    (increase + 1) * "L_QUANTITY" * popularity AS pipsales,
--    "L_PARTKEY" AS part
--  FROM
--    "LINEITEM","ORDERS",
--    (SELECT
--      part,popularity,pip_value_bundle_create(popularity, 60000) AS mcdbpopularity
--    FROM
--      (SELECT
--        "P_PARTKEY" AS part,
--        CREATE_VARIABLE('Exponential', row(1)) AS popularity
--      FROM
--        "PART"
--      WHERE
--        "P_PARTKEY" <= 50) AS pp
--    WHERE
--      popularity > 5.29) partpopularity,
--    (SELECT
--      newcount/oldcount AS potincrease,
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
--  ORDER BY
--    part;

SELECT
  part, sum(realsales * 0.00504) as correct, sum(pip_value_bundle_expect(mcdbsales, mcdbpopularity)) as mcdbsoe, pip_value_bundle_expect(sum(mcdbsales,mcdbpopularity)) as mcdbeos
FROM 
  increase_per_part
WHERE part < 5
GROUP BY part
ORDER BY part;