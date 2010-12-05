CREATE TABLE R(A int, B int) 
  FROM FILE 'test/data/r.dat' LINE DELIMITED
  csv (fields := ',', schema := 'int,int', eventtype := 'insert');

CREATE TABLE S(B int, C int) 
  FROM FILE 'test/data/s.dat' LINE DELIMITED
  csv (fields := ',', schema := 'int,int', eventtype := 'insert');

CREATE TABLE T(C int, D int)
  FROM FILE 'test/data/t.dat' LINE DELIMITED
  csv (fields := ',', schema := 'int,int', eventtype := 'insert');

-- Example of a nested query with a join.
SELECT   sum (R.A)
FROM     R
WHERE    (select sum(S.B + T.D) FROM S,T WHERE S.C=T.C AND R.B < S.B) > 1000;
