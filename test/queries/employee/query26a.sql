-- List out the no.of employees for each month and year, in the ascending order based on the year, month.

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

SELECT DATE_PART('year', hire_date) AS hire_year, DATE_PART('month', hire_date) AS hire_month, count(*) AS no_of_employees
FROM employee 
GROUP BY hire_year, hire_month