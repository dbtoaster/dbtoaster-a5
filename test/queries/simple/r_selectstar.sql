-- Expected result: 


CREATE STREAM R(A string, B string) 
  FROM FILE '../../experiments/data/tiny_r.dat' LINE DELIMITED
  csv (fields := ',', schema := 'string,string', eventtype := 'insert');

SELECT * FROM R;