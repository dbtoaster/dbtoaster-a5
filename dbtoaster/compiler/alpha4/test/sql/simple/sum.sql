CREATE TABLE R(A int, B int) 
  FROM FILE '../../experiments/data/r.dat' LINE DELIMITED csv (fields := ',', schema := 'int,int', eventtype := 'insert');

SELECT sum(A) FROM R;
