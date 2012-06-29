-- Which is the department id, having greater than or equal to 3 employees joined in April 1985.

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

SELECT department_id, count(*) AS no_of_employees
FROM employee 
WHERE year(hire_date)=1985 month(hire_date)=4 
GROUP BY year(hire_date), month(hire_date), department_id 
HAVING count(*)>=3
