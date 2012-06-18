-- Find out the employees who earn greater than the average salary for their department.

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

CREATE STREAM DEPARTMENT(
    department_id   INT,
    name            VARCHAR(20),
    location_id     INT
    ) 
  FROM FILE '../../experiments/data/employee/department.dat' LINE DELIMITED
  CSV (fields := ',');

SELECT e1.employee_id, e1.last_name, e1.salary, e1.department_id 
FROM employee e1 
WHERE e1.salary > (SELECT avg(e2.salary) 
                   FROM employee e2 
                   WHERE e1.department_id=e2.department_id)
