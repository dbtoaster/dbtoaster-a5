CREATE STREAM R(A int, B int) FROM FILE '../dbtoaster-experiments-data/simple/tiny/r.dat' LINE DELIMITED CSV();

CREATE STREAM S(B int, C int) FROM FILE '../dbtoaster-experiments-data/simple/tiny/s.dat' LINE DELIMITED CSV();

CREATE STREAM T(C int, D int) FROM FILE '../dbtoaster-experiments-data/simple/tiny/t.dat' LINE DELIMITED CSV();

SELECT mWhzshHy.C AS va7dY72fZ, 6 AS kCguQGvFE, 5 AS SKg5wbLv, 5 AS buf2m, (mWhzshHy.D+mWhzshHy.D) AS PsUF0zGir, 79.16 AS bgAEZ FROM S bIR1_PL, T Umg5YQ3HG, T mWhzshHy WHERE Umg5YQ3HG.C = mWhzshHy.D;
