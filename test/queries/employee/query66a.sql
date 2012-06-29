-- List out the common jobs in Research and Accounting Departments in ascending order.

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

CREATE STREAM DEPARTMENT(
    department_id   INT,
    name            VARCHAR(20),
    location_id     INT
    ) 
  FROM FILE '../../experiments/data/employee/department.dat' LINE DELIMITED
  CSV ();

CREATE STREAM JOB(
    job_id      INT,
    function    VARCHAR(20)
    ) 
  FROM FILE '../../experiments/data/employee/job.dat' LINE DELIMITED
  CSV ();

SELECT function 
FROM job j1, (SELECT e1.job_id
              FROM employee e1
              WHERE e1.department_id = (SELECT d1.department_id 
                                        FROM department d1 
                                        WHERE d1.name = 'RESEARCH')) j2,
             (SELECT e2.job_id 
              FROM employee e2
              WHERE e2.department_id = (SELECT d2.department_id 
                                        FROM department d2
                                        WHERE d2.name = 'ACCOUNTING')) j3                     
WHERE j1.job_id = j2.job_id AND j1.job_id = j3.job_id
