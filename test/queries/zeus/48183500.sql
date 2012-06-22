CREATE STREAM R(A int, B int) FROM FILE '../../experiments/data/simple/tiny/r.dat' LINE DELIMITED CSV();

CREATE STREAM S(B int, C int) FROM FILE '../../experiments/data/simple/tiny/s.dat' LINE DELIMITED CSV();

CREATE STREAM T(C int, D int) FROM FILE '../../experiments/data/simple/tiny/t.dat' LINE DELIMITED CSV();

SELECT *, (S.C/68.12) AS RB1M0UL, 3 AS AU_D7, S.C AS Q8EhFQvS3, 87.0 AS pOoF4jqb, bV0ZrLqKV.A AS NkmbTt1vV, ((45.75*4)*97.76*S.C) AS wcwZwifh FROM R bV0ZrLqKV, S, S oYxB_E WHERE ((((5+(bV0ZrLqKV.A-S.C)+7)+bV0ZrLqKV.A+8) = S.B OR 0 < (((oYxB_E.B-8)-(oYxB_E.B/bV0ZrLqKV.B))*7)) AND (NOT oYxB_E.C = bV0ZrLqKV.B));
