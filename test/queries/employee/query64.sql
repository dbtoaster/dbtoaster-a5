-- List out the distinct jobs in Sales and Accounting Departments.

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

CREATE STREAM DEPARTMENT(
    department_id   INT,
    name            VARCHAR(20),
    location_id     INT
    ) 
  FROM FILE '../dbtoaster-experiments-data/employee/department.dat' LINE DELIMITED
  CSV ();

CREATE STREAM JOB(
    job_id      INT,
    job_function    VARCHAR(20)
    ) 
  FROM FILE '../dbtoaster-experiments-data/employee/job.dat' LINE DELIMITED
  CSV ();


SELECT job_function 
FROM job 
WHERE job_id IN (SELECT job_id 
                 FROM employee 
                 WHERE department_id = (SELECT department_id 
                                        FROM department 
                                        WHERE name = 'SALES')) 
UNION
SELECT job_function 
FROM job 
WHERE job_id IN (SELECT job_id 
                 FROM employee 
                 WHERE department_id = (SELECT department_id 
                                        FROM department 
                                        WHERE name = 'ACCOUNTING'))
