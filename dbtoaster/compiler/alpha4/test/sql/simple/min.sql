CREATE TABLE R(A int, B int) 
  FROM FILE 'test/data/r.dat' LINE DELIMITED
  csv (fields := ',', schema := 'int,int', eventtype := 'insert');

-- This query computes
-- SELECT min(A) FROM R

-- The query says 45337 is the correct result. The domain is too large.
  
SELECT r1.A, sum(1)
FROM R r1
WHERE (SELECT sum(1) from R r2 where r1.A > r2.A) = 0
GROUP BY r1.A;
