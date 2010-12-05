-- SQL query that results in a statement with 2 input maps
-- with different variables, i.e.:
--
-- ( QUERY_1_1[  ][  ] := 0. ) +=
--  ( ( QUERY_1_1S1[ QUERY_1_1R1T1_initS_S__B ][  ] := 
--      ( IF ( ( R__B < QUERY_1_1R1T1_initS_S__B ) )
--        THEN ( ( QUERY_1_1S1_init[  ][ R__B ] := 0. ) )) )
--     * 
--    ( QUERY_1_1R1S1[ QUERY_1_1R1T1_initS_S__C ][  ] := 
--      ( IF ( ( QUERY_1_1R1T1_initS_S__C < T__C ) )
--        THEN ( ( QUERY_1_1R1S1_init[  ][ T__C ] := 0. ) )) )
--   )

CREATE TABLE R(A int, B int) 
  FROM FILE 'test/data/r.dat' LINE DELIMITED
  csv (fields := ',', schema := 'int,int', eventtype := 'insert');

CREATE TABLE S(B int, C int) 
  FROM FILE 'test/data/s.dat' LINE DELIMITED
  csv (fields := ',', schema := 'int,int', eventtype := 'insert');

CREATE TABLE T(C int, D int)
  FROM FILE 'test/data/t.dat' LINE DELIMITED
  csv (fields := ',', schema := 'int,int', eventtype := 'insert');

SELECT sum(A*D) FROM R,S,T WHERE R.B<S.B AND S.C<T.C;
