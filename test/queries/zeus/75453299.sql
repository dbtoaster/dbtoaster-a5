CREATE STREAM R(A int, B int) FROM FILE '../dbtoaster-experiments-data/simple/tiny/r.dat' LINE DELIMITED CSV();

CREATE STREAM S(B int, C int) FROM FILE '../dbtoaster-experiments-data/simple/tiny/s.dat' LINE DELIMITED CSV();

CREATE STREAM T(C int, D int) FROM FILE '../dbtoaster-experiments-data/simple/tiny/t.dat' LINE DELIMITED CSV();

SELECT S2eMVA0L.B AS waL3C_iV, ((S2eMVA0L.B+2)-S2eMVA0L.B) AS jpWDb, 85.06 AS qhiQArqf5 FROM S S2eMVA0L WHERE (NOT S2eMVA0L.C >= S2eMVA0L.B);
