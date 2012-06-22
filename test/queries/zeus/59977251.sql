CREATE STREAM R(A int, B int) FROM FILE '../../experiments/data/simple/tiny/r.dat' LINE DELIMITED CSV();

CREATE STREAM S(B int, C int) FROM FILE '../../experiments/data/simple/tiny/s.dat' LINE DELIMITED CSV();

CREATE STREAM T(C int, D int) FROM FILE '../../experiments/data/simple/tiny/t.dat' LINE DELIMITED CSV();

SELECT 0 AS SU7wzb, 0 AS ksDf5LpM, T.C AS ayedm, (T.C*T.C*12.82) AS C8ubb, T.C AS C6Dqtc FROM T WHERE (NOT (NOT T.C = T.C));
