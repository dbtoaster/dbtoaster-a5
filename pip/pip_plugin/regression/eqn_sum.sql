ALTER SEQUENCE pip_var_id RESTART WITH 1;

CREATE TEMPORARY TABLE inttable(V int);
INSERT INTO inttable VALUES 
  (1),(2),(3),(4),(5);

CREATE TEMPORARY TABLE vartable AS
  SELECT 
    A.V,
    CREATE_VARIABLE('Exponential', row(B.V)) AS var
  FROM
    inttable A, inttable B
  WHERE
    B.V <= A.V;

SELECT   V, var
FROM     vartable;

SELECT   V, sum(var), << sum(var) >>
FROM     vartable
GROUP BY V
ORDER BY V;