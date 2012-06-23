-- Unsupported features for this query
--   INTERVAL (inlined into constant)

-- Note that this query will fail to produce the correct answer on the OCaml 
-- interpreter due to an error in OCaml's handling of floating-point addition.
-- Specifically (0.06+0.01) is not less than or equal to 0.07.
-- Replace 0.06+/-0.01 below with a constant to get the right answer.

INCLUDE 'test/queries/tpch/schemas.sql';

SELECT SUM(l.extendedprice*l.discount) AS revenue
FROM   lineitem l
WHERE  l.shipdate >= DATE('1994-01-01')
  AND  l.shipdate < DATE('1995-01-01')
  AND  (l.discount BETWEEN (0.06 - 0.01) AND (0.06 + 0.01)) 
  AND  l.quantity < 24;