-- Display the employee who got the maximum salary.

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

SELECT * 
FROM employee 
WHERE salary = (SELECT max(salary) FROM employee)
