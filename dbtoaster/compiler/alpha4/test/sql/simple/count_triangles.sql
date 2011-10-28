-- Computes the number of triangles in a binary relation. The correct answer is four.

CREATE TABLE R(A int, B int) 
  FROM FILE 'test/data/r_4triangles.dat' LINE DELIMITED
  csv (fields := ',', schema := 'int,int', eventtype := 'insert');

SELECT sum(1) FROM R r1, R r2, R r3 WHERE r1.B=r2.A AND r1.A=r3.A AND r2.B=r3.B;

