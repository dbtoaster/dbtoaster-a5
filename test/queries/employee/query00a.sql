--  Display employee details with salary grades.

CREATE STREAM SALARY_GRADE(
    grade_id     INT, 
    lower_bound  FLOAT,
    upper_bound  FLOAT
    ) 
  FROM FILE '../../experiments/data/employee/salary_grade.dat' LINE DELIMITED
  CSV (fields := ',');

SELECT grade_id, lower_bound, upper_bound 
FROM salary_grade
