\timing
ALTER SEQUENCE pip_var_id RESTART WITH 1;

---------------------- Initialization -------------------------------

--DROP TABLE IF EXISTS absolute_correct_profit;
--CREATE TABLE absolute_correct_profit AS
--  SELECT
--    (increase + 1) * "L_QUANTITY" * 0.0317 AS salesavg,
--    "L_PARTKEY" AS part
--  FROM
--    "LINEITEM","ORDERS",
--    (SELECT
--      newcount/oldcount AS increase, customer
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
--    "O_CUSTKEY"=customer AND "O_ORDERKEY" = "L_ORDERKEY";


--DROP TABLE IF EXISTS increase_per_part;
--CREATE TABLE increase_per_part AS
--  SELECT
--    (increase + 1) * "L_QUANTITY" * popularity AS sales,
--    "L_PARTKEY" AS part
--  FROM
--    "LINEITEM","ORDERS",
--    (SELECT
--      part,popularity
--    FROM
--      (SELECT
--        "P_PARTKEY" AS part,
--        CREATE_VARIABLE('Exponential', row(1)) AS popularity
--      FROM
--        "PART"
--      WHERE
--        "P_PARTKEY" <= 5000) AS pp
--    WHERE
--      popularity > 5.29) partpopularity,
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
--    "O_CUSTKEY"=customer AND "O_ORDERKEY" = "L_ORDERKEY" AND "L_PARTKEY"=partpopularity.part;

CREATE TEMPORARY TABLE sample_count(cnt integer);
--INSERT INTO sample_count(cnt) VALUES (1);
--INSERT INTO sample_count(cnt) VALUES (2);
--INSERT INTO sample_count(cnt) VALUES (5);
--INSERT INTO sample_count(cnt) VALUES (10);
INSERT INTO sample_count(cnt) VALUES (25);
INSERT INTO sample_count(cnt) VALUES (50);
INSERT INTO sample_count(cnt) VALUES (100);
--INSERT INTO sample_count(cnt) VALUES (250);
--INSERT INTO sample_count(cnt) VALUES (500);
--INSERT INTO sample_count(cnt) VALUES (1000);
CREATE TEMPORARY TABLE sample_instance(num integer);
INSERT INTO sample_instance(num) VALUES 
  (00), (01), (02), (03), (04), (05), (06), (07), (08), (09),
  (10), (11), (12), (13), (14), (15), (16), (17), (18), (19),
  (20), (21), (22), (23), (24), (25), (26), (27), (28), (29);--,
--  (30), (31), (32), (33), (34), (35), (36), (37), (38), (39),
--  (40), (41), (42), (43), (44), (45), (46), (47), (48), (49),
--  (50), (51), (52), (53), (54), (55), (56), (57), (58), (59),
--  (60), (61), (62), (63), (64), (65), (66), (67), (68), (69),
--  (70), (71), (72), (73), (74), (75), (76), (77), (78), (79),
--  (80), (81), (82), (83), (84), (85), (86), (87), (88), (89),
--  (90), (91), (92), (93), (94), (95), (96), (97), (98), (99);
CREATE TEMPORARY TABLE samplepoints AS
  SELECT * FROM sample_count, sample_instance;

--DROP TABLE IF EXISTS sample_increases;
--CREATE TABLE sample_increases AS
--  SELECT
--    part,
--    num,cnt,
--    expectation_sum(sales, increase_per_part, cnt) AS sales
--  FROM
--    increase_per_part,
--    samplepoints
--  GROUP BY
--    part,num,cnt;

--DROP TABLE IF EXISTS part_probabilities;
--CREATE TABLE part_probabilities AS
--  SELECT
--    part,
--    expectation_sum(sales, increase_per_part, 20000) AS salesavg
--  FROM
--    increase_per_part
--  GROUP BY
--    part;
    
DROP TABLE IF EXISTS dev_per_part;
CREATE TABLE dev_per_part AS
  SELECT
    part, cnt, sqrt(avg((sales-salesavg)*(sales-salesavg)/(salesavg*salesavg))) AS salesdev
  FROM
    sample_increases
    NATURAL JOIN
    absolute_correct_profit
  GROUP BY
    part, cnt;

DROP TABLE IF EXISTS samples;
CREATE TABLE samples AS
  SELECT
    cnt, avg(salesdev)
  FROM
    dev_per_part
    NATURAL JOIN
    absolute_correct_profit
  WHERE
    salesavg < 'infinity'
  GROUP BY
    cnt;
      
      
SELECT * FROM samples;