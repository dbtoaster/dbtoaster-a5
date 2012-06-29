-- List out the employees who earn more than the lowest salary in department 30.

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

SELECT * 
FROM employee e1 
WHERE e1.salary > ANY (SELECT e2.salary 
                       FROM employee e2 
                       WHERE department_id=30)
