-- Expected result: 


CREATE STREAM R(A int, B int) 
  FROM FILE '../../experiments/data/tiny_r.dat' LINE DELIMITED
  CSV (fields := ',');

CREATE STREAM S(B int, C int) 
  FROM FILE '../../experiments/data/tiny_s.dat' LINE DELIMITED
  CSV (fields := ',');

SELECT r.A, SUM(s.C)
FROM R r, S s
WHERE r.B = S.B
GROUP BY r.A;
