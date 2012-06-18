CREATE STREAM R(A int, B int) 
  FROM FILE '../../experiments/data/tiny_r.dat' LINE DELIMITED
  CSV (fields := ',');
  
CREATE STREAM S(B int, C int) 
  FROM FILE '../../experiments/data/tiny_s.dat' LINE DELIMITED
  CSV (fields := ',');



SELECT R.A,R.B FROM R WHERE  R.A < ( SELECT SUM(S.C) FROM S WHERE R.B = S.B );




/*
SELECT sum(A+B) FROM R;

SELECT sum(R.A) FROM R WHERE R.B = (SELECT sum(S.C) FROM S);
*/
