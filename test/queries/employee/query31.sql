-- How many employees joined each month in 1985.

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

SELECT DATE_PART('year', hire_date), DATE_PART('month', hire_date), count(*) AS no_of_employees
FROM employee 
WHERE DATE_PART('year', hire_date) = 1985 
GROUP BY DATE_PART('year', hire_date), DATE_PART('month', hire_date)
