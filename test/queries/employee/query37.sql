-- Display the employees who are working in "Chicago".

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
  csv (fields := ',', schema := 'int,string,string,string,int,int,date,float,float,int', eventtype := 'insert');

CREATE STREAM DEPARTMENT(
    department_id   INT,
    name            VARCHAR(20),
    location_id     INT
    ) 
  FROM FILE '../../experiments/data/employee/department.dat' LINE DELIMITED
  csv (fields := ',', schema := 'int,string,int', eventtype := 'insert');

CREATE STREAM LOCATION(
    location_id      INT,
    regional_group   VARCHAR(20)
    ) 
  FROM FILE '../../experiments/data/employee/location.dat' LINE DELIMITED
  csv (fields := ',', schema := 'int,string', eventtype := 'insert');

SELECT * 
FROM employee e
WHERE e.department_id IN (SELECT d.department_id 
                       FROM department d 
                       WHERE d.location_id IN (SELECT l.location_id 
                                            FROM location l 
                                            WHERE l.regional_group='CHICAGO'))
                                            