-- Display all employees in sales or operation departments.

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


SELECT last_name, d.department_id, d.name 
FROM employee e LEFT JOIN department d 
ON e.department_id = d.department_id 
WHERE d.department_id IN (SELECT department_id 
                          FROM department 
                          WHERE name IN ('SALES','OPERATIONS'))
