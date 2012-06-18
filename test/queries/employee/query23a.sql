-- List out the department wise average salary of the employees

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
  CSV (fields := ',');

SELECT department_id, count(*) AS total_employees, 
       sum(salary) AS total_salary, sum(salary)/sum(1) AS avg_salary 
FROM employee 
GROUP BY department_id
