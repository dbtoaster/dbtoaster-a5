CREATE TABLE R(A int, B int) 
  FROM FILE '../../experiments/data/tiny/r.dat' LINE DELIMITED
  CSV ();

CREATE STREAM S(B int, C int) 
  FROM FILE '../../experiments/data/tiny/s.dat' LINE DELIMITED
  CSV ();

SELECT SUM(r.A*s.C) as RESULT FROM R r, S s WHERE r.B = s.B;
