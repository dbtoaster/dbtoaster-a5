DROP TABLE IF EXISTS R;
CREATE TABLE R(A bigint, B bigint);

DROP TABLE IF EXISTS S;
CREATE TABLE S(B bigint, C bigint);

DROP TABLE IF EXISTS T;
CREATE TABLE T(C bigint, D bigint);

COPY R FROM '@@PATH@@/test/data/r.dat' WITH DELIMITER ',';
COPY S FROM '@@PATH@@/test/data/s.dat' WITH DELIMITER ',';
COPY T FROM '@@PATH@@/test/data/t.dat' WITH DELIMITER ',';

SELECT S.B,S.C,sum(A*D) FROM R,S,T WHERE R.B<S.B AND S.C<T.C GROUP BY S.B,S.C;
