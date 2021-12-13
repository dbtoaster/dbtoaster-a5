-- List out the employees who are working in department 10 and draw the salaries more than 3500.

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
  FROM FILE '../dbtoaster-experiments-data/employee/employee.dat' LINE DELIMITED
  CSV ();

SELECT employee_id, last_name, first_name, middle_name, 
       job_id, manager_id, hire_date, salary, commission, department_id 
FROM employee
WHERE department_id=30 AND salary>2750;
