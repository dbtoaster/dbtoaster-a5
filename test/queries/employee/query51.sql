--  How many employees who are working in different departments and display with department name.

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

CREATE STREAM LOCATION(
    location_id      INT,
    regional_group   VARCHAR(20)
    ) 
  FROM FILE '../../experiments/data/employee/location.dat' LINE DELIMITED
  CSV ();


SELECT name, count(*) 
FROM employee e, department d
WHERE d.department_id=e.department_id 
GROUP BY name
