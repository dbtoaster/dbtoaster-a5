-- Display the employees with their department name and regional groups.

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

CREATE STREAM LOCATION(
    location_id      INT,
    regional_group   VARCHAR(20)
    ) 
  FROM FILE '../dbtoaster-experiments-data/employee/location.dat' LINE DELIMITED
  CSV ();


SELECT employee_id, last_name, name, regional_group 
FROM employee e, department d, location l
WHERE e.department_id=d.department_id AND d.location_id=l.location_id
