--  Display employee details with salary grades.

CREATE STREAM SALARY_GRADE(
    grade_id     INT, 
    lower_bound  FLOAT,
    upper_bound  FLOAT
    ) 
  FROM FILE '../../experiments/data/employee/salary_grade.dat' LINE DELIMITED
  csv (fields := ',', schema := 'int,float,float', eventtype := 'insert');

SELECT * 
FROM salary_grade