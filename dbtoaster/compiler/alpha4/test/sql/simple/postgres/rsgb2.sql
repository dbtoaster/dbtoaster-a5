DROP TABLE IF EXISTS R;
CREATE TABLE R(A int, B int);

DROP TABLE IF EXISTS S;
CREATE TABLE S(B int, C int);

COPY R FROM '@@PATH@@/test/data/r.dat' WITH DELIMITER ',';
COPY S FROM '@@PATH@@/test/data/s.dat' WITH DELIMITER ',';

SELECT C,sum(A) FROM R,S WHERE R.B<S.B group by C;
