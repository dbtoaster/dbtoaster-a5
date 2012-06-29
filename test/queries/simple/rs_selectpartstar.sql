-- Expected result: 

CREATE STREAM R(A string, B string) 
  FROM FILE '../../experiments/data/simple/tiny/r.dat' LINE DELIMITED
  CSV ();

CREATE STREAM S(B int, C int) 
  FROM FILE '../../experiments/data/simple/tiny/s.dat' LINE DELIMITED
  CSV ();

SELECT R.* FROM R, S;
