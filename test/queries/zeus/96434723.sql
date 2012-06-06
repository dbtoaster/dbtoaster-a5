CREATE STREAM R(A int, B int) FROM FILE '../../experiments/data/tiny_r.dat' LINE DELIMITED CSV();

CREATE STREAM S(B int, C int) FROM FILE '../../experiments/data/tiny_s.dat' LINE DELIMITED CSV();

CREATE STREAM T(C int, D int) FROM FILE '../../experiments/data/tiny_t.dat' LINE DELIMITED CSV();

SELECT 
--  S.C, 
  nPzL_7DkV.B, 
  nPzL_7DkV.C, 
--  69.81 AS oIpRp0Cj, 
--  4 AS J5TuuZhG7, 
--  2 AS BClqo, 
  (nPzL_7DkV.B+(S.B*S.C)) AS LB71Y, 
--  3 AS V2rjPyu0, 
--  nPzL_7DkV.C AS c6w05qW, 
--  (S.C*S.C*S.B) AS k9uBsiBqT, 
--  (nPzL_7DkV.B-S.C) AS PhxN4WeTw, 
--  2 AS AtlY5kpO, 
--  S.B AS U4r8Z, 
--  ((S.C*nPzL_7DkV.C)+(((7-nPzL_7DkV.B)*nPzL_7DkV.C)+S.C+nPzL_7DkV.B)+nPzL_7DkV.B) AS rcjvJT2, 
--  (S.B*nPzL_7DkV.B*13.48) AS jl7xW, 
--  4 AS sbWQ1B, 
  (nPzL_7DkV.C*nPzL_7DkV.B*(1*nPzL_7DkV.B)) AS gkkEoF, 
--  nPzL_7DkV.B AS gbrFybc, 
--  9 AS s1CSC, 
  S.C AS qKKF7PYi 
FROM S s1, S nPzL_7DkV 
WHERE 
  (NOT 2 = nPzL_7DkV.C) 
  AND 0 = nPzL_7DkV.B
--  AND (S.C = S.B AND nPzL_7DkV.C < nPzL_7DkV.B));
