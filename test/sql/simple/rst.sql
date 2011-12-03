-- With the datafiles in ../../experiments/data, this should produce
-- QUERY_1_1 = 18753367048934
-- or as the Ocaml outputter puts it:
-- QUERY_1_1: [[  ]->[[  ]->1.87533670489e+13;]<pat=>;]<pat=>

CREATE TABLE R(A int, B int) 
  FROM FILE '../../experiments/data/big_r.dat' LINE DELIMITED
  csv (fields := ',', schema := 'int,int', eventtype := 'insert');

CREATE TABLE S(B int, C int) 
  FROM FILE '../../experiments/data/big_s.dat' LINE DELIMITED
  csv (fields := ',', schema := 'int,int', eventtype := 'insert');

CREATE TABLE T(C int, D int)
  FROM FILE '../../experiments/data/big_t.dat' LINE DELIMITED
  csv (fields := ',', schema := 'int,int', eventtype := 'insert');

SELECT sum(A*D) AS result FROM R,S,T WHERE R.B=S.B AND S.C=T.C;
