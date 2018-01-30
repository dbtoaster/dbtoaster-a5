-- How many employees who are joined in January or September month.

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

SELECT DATE_PART('month', hire_date), count(*) 
FROM employee 
GROUP BY DATE_PART('month', hire_date) 
HAVING DATE_PART('month', hire_date) IN (1, 9)
