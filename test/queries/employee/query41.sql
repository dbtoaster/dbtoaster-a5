-- Display the second highest salary drawing employee details.

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

SELECT * 
FROM employee e1
WHERE e1.salary=(SELECT max(e2.salary) 
                 FROM employee e2 
                 WHERE e2.salary < (SELECT max(e3.salary) 
                                    FROM employee e3))
