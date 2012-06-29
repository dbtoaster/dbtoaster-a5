
CREATE STREAM R(A int, B int) 
  FROM FILE '../../experiments/data/simple/tiny/r.dat' LINE DELIMITED
  CSV ();

SELECT r1.B
FROM R r1, (SELECT 1 AS B) r1
