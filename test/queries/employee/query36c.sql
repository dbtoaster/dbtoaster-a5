-- Display the employees who are working as "Clerk".

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

CREATE STREAM JOB(
    job_id      INT,
    function    VARCHAR(20)
    ) 
  FROM FILE '../../experiments/data/employee/job.dat' LINE DELIMITED
  CSV (fields := ',');

SELECT * 
FROM job j JOIN employee e 
ON j.job_id = e.job_id
WHERE function='CLERK'
