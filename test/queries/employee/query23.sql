-- List out the department wise maximum salary, minimum salary, average salary of the employees

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
  csv (fields := ',', schema := 'int,string,string,string,int,int,date,float,float,int', eventtype := 'insert');

SELECT department_id, count(*), max(salary), min(salary), avg(salary) 
FROM employee 
GROUP BY department_id