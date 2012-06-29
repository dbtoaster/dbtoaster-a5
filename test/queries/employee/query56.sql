--  Display employee details with salary grades.

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

SELECT employee_id, last_name, grade_id 
FROM employee e, salary_grade s 
WHERE salary BETWEEN lower_bound AND upper_bound 
ORDER BY last_name
