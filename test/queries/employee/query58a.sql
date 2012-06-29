-- Display the employ salary grades and no. of employees between 2000 to 5000 range of salary.

CREATE STREAM EMPLOYEE(
    employee_id     INT, 
    last_name       VARCHAR(30),
    first_name      VARCHAR(20),
    middle_name     CHAR(1),
    job_id          INT,
    manager_id      INT,
    hire_date       DATE,
    salary          FLOAT,
    commission      FLOAT,
    department_id   INT
    ) 
  FROM FILE '../../experiments/data/employee/employee.dat' LINE DELIMITED
  CSV ();

CREATE STREAM SALARY_GRADE(
    grade_id     INT, 
    lower_bound  FLOAT,
    upper_bound  FLOAT
    ) 
  FROM FILE '../../experiments/data/employee/salary_grade.dat' LINE DELIMITED
  CSV ();

SELECT grade_id, count(*) 
FROM employee e, salary_grade s 
WHERE salary BETWEEN lower_bound AND upper_bound AND 
      lower_bound >= 2000 AND lower_bound <= 5000 
GROUP BY grade_id 
