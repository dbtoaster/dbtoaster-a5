--  Which is the department having greater than or equal to 2 employees and display the department names in ascending order.

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

CREATE STREAM DEPARTMENT(
    department_id   INT,
    name            VARCHAR(20),
    location_id     INT
    ) 
  FROM FILE '../../experiments/data/employee/department.dat' LINE DELIMITED
  CSV (fields := ',');

SELECT a.name, a.count_number
FROM (SELECT d.name, count(*) AS count_number 
      FROM employee e, department d
      WHERE d.department_id = e.department_id 
      GROUP BY d.name) a
WHERE a.count_number >= 2
