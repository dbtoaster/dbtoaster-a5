CREATE STREAM R(A int, B int)
FROM FILE '../../experiments/data/tiny_r.dat' LINE DELIMITED
CSV (fields := ',');

CREATE STREAM S(B int, C int)
FROM FILE '../../experiments/data/tiny_s.dat' LINE DELIMITED
CSV (fields := ',');

SELECT * FROM R,S WHERE R.B = S.B AND R.A < S.C