\timing
ALTER SEQUENCE pip_var_id RESTART WITH 1;

SELECT
  max(production, prod)
FROM
  (
    SELECT
      *
    FROM
      ( 
        SELECT
          "L_SUPPKEY" as supplier,
          sum("L_QUANTITY") as production,
          CREATE_VARIABLE('Poisson', row(1)) as reliability
        FROM
          "LINEITEM"
        GROUP BY 
          supplier
        ORDER BY
          production desc
      ) prod
    WHERE
      reliability < 1.0
  ) prod;


--You can invest in a vast array of suppliers with varying degrees of effort. There is some function defining the probability of a given investment coming through.  In the end, you pick the best one and 
