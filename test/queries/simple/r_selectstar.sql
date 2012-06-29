-- Expected result: 


CREATE STREAM R(A string, B string) 
  FROM FILE '../../experiments/data/simple/tiny/r.dat' LINE DELIMITED
  CSV ();

SELECT * FROM R;
