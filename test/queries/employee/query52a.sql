--  How many employees who are working in sales department.

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

SELECT a.name, a.c
FROM (SELECT d.name, count(*) as c
      FROM employee e, department d
      WHERE d.department_id=e.department_id 
      GROUP BY name) a
WHERE a.name='SALES'
