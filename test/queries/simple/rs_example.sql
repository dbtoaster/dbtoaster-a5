CREATE TABLE R(A int, B int) 
  FROM FILE '../../experiments/data/simple/tiny/r.dat' LINE DELIMITED
  CSV (fields := ',');

CREATE STREAM S(B int, C int) 
  FROM FILE '../../experiments/data/simple/tiny/s.dat' LINE DELIMITED
  CSV (fields := ',');

SELECT SUM(r.A*s.C) as Result FROM R r, S s WHERE r.B = s.B;
