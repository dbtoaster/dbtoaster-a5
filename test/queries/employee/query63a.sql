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


SELECT last_name, d1.department_id, d1.name 
FROM employee e, department d1, 
     (SELECT d3.department_id
      FROM department d3
      WHERE d3.name = 'SALES' OR d3.name = 'OPERATIONS') d2
WHERE e.department_id = d1.department_id AND
      d1.department_id = d2.department_id
