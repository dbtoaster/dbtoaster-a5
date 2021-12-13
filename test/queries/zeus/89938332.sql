CREATE STREAM R(A int, B int) FROM FILE '../dbtoaster-experiments-data/simple/tiny/r.dat' LINE DELIMITED CSV();

CREATE STREAM S(B int, C int) FROM FILE '../dbtoaster-experiments-data/simple/tiny/s.dat' LINE DELIMITED CSV();

CREATE STREAM T(C int, D int) FROM FILE '../dbtoaster-experiments-data/simple/tiny/t.dat' LINE DELIMITED CSV();

SELECT (8*2) AS D0zaYgQ, (T.D*(7-54.59)*(EPZEX.D*(44.14*4)*0)) AS z__D4 FROM T, T EPZEX WHERE 0 = T.C;
