--  List out employee_id, last_name, department_id for all employees and 
--  rename employee_id as "eid", last_name as "name", department_id as "did"

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

SELECT employee_id AS eid, last_name AS name, department_id AS did  
FROM employee;
