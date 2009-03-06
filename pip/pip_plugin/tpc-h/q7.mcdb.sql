\timing
ALTER SEQUENCE pip_var_id RESTART WITH 1;

---------------------- Initialization -------------------------------

DROP TABLE IF EXISTS increase_per_part_mcdb;
CREATE TABLE increase_per_part_mcdb AS
  SELECT
    "L_PARTKEY" AS part,
    sum(
      pip_value_bundle_mul(
        pip_value_bundle_mul(
          pip_value_bundle_add(
            pip_value_bundle_create(increase, 60000), 
            1.0),
          "L_QUANTITY"),
        popularity),
      worldpresence) AS sales
  FROM
    "LINEITEM","ORDERS",
    (SELECT
      part,popularity,
      pip_value_bundle_cmp(pip_world_presence_create(60000), popularity, 5.29) AS worldpresence
    FROM
      (SELECT
        "P_PARTKEY" AS part,
        pip_value_bundle_create(CREATE_VARIABLE('Exponential', row(1)), 60000) AS popularity
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


CREATE TEMPORARY TABLE sample_count(cnt integer, delta integer);

--INSERT INTO sample_count(cnt,delta) VALUES (1,0);
--INSERT INTO sample_count(cnt,delta) VALUES (2,30);
--INSERT INTO sample_count(cnt,delta) VALUES (5,90);
--INSERT INTO sample_count(cnt,delta) VALUES (10,240);
--INSERT INTO sample_count(cnt,delta) VALUES (25,540);
--INSERT INTO sample_count(cnt,delta) VALUES (50,1390);
--INSERT INTO sample_count(cnt,delta) VALUES (100,2790);
--INSERT INTO sample_count(cnt,delta) VALUES (250,5790);
INSERT INTO sample_count(cnt,delta) VALUES (500,13290);
INSERT INTO sample_count(cnt,delta) VALUES (1000,28290);

CREATE TEMPORARY TABLE sample_instance(num integer);
INSERT INTO sample_instance(num) VALUES 
  (00), (01), (02), (03), (04), (05), (06), (07), (08), (09),
  (10), (11), (12), (13), (14), (15), (16), (17), (18), (19),
  (20), (21), (22), (23), (24), (25), (26), (27), (28), (29);--,

CREATE TEMPORARY TABLE samplepoints AS
  SELECT 
    cnt, cnt*num+delta AS low, (cnt+1)*num+delta AS high
  FROM 
    sample_count, sample_instance;

SELECT
  cnt, avg(salesdev/salesavg)
FROM
  (SELECT
    part, cnt, stddev(sales) AS salesdev
  FROM 
    (SELECT
      part, cnt,
      pip_value_bundle_expect(sales, low, high) AS sales
    FROM
      increase_per_part_mcdb, samplepoints) AS sample_increases
  GROUP BY
    part, cnt) AS dev_per_part
  NATURAL JOIN
  (SELECT
    part, cnt,
    pip_value_bundle_expect(sales) AS salesavg
  FROM
    increase_per_part_mcdb, sample_count
  ) AS avg_per_part
WHERE
  salesavg < 'Infinity'
GROUP BY
  cnt;
