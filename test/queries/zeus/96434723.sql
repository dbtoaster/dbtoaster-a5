CREATE STREAM R(A int, B int) FROM FILE '../dbtoaster-experiments-data/simple/tiny/r.dat' LINE DELIMITED CSV();

CREATE STREAM S(B int, C int) FROM FILE '../dbtoaster-experiments-data/simple/tiny/s.dat' LINE DELIMITED CSV();

CREATE STREAM T(C int, D int) FROM FILE '../dbtoaster-experiments-data/simple/tiny/t.dat' LINE DELIMITED CSV();

SELECT 
  nPzL_7DkV.B AS B1, 
  nPzL_7DkV.C AS C1, 
  (nPzL_7DkV.B+(s1.B*s1.C)) AS LB71Y, 
  (nPzL_7DkV.C*nPzL_7DkV.B*(1*nPzL_7DkV.B)) AS gkkEoF, 
  S1.C AS qKKF7PYi 
FROM S s1, S nPzL_7DkV 
WHERE 
  (NOT 2 = nPzL_7DkV.C) 
  AND 0 = nPzL_7DkV.B
--  AND (S.C = S.B AND nPzL_7DkV.C < nPzL_7DkV.B));
