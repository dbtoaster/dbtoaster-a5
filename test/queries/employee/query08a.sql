--  List the details about "SMITH".

CREATE STREAM EMPLOYEE(
    employee_id     INT, 
    last_name       VARCHAR(30),
    first_name      VARCHAR(20),
    middle_name     CHAR(1),
    job_id          INT,
    manager_id      INT,
    hire_date        DATE,
    salary          FLOAT,
    commission      FLOAT,
    department_id   INT
    ) 
  FROM FILE '../../experiments/data/employee/employee.dat' LINE DELIMITED
  CSV (fields := ',');

SELECT employee_id, last_name, first_name, middle_name, 
       job_id, manager_id, hire_date, salary, commission, department_id 
FROM employee 
WHERE last_name='SMITH';
