-- List out the job wise average salaries of the employees.

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

SELECT job_id, count(*) AS job_count, sum(salary) AS total_salary, 
       sum(salary)/count(*) AS avg_salary 
FROM employee 
GROUP BY job_id
