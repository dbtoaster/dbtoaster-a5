-- Display the employee details who earn more than their managers salaries.

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

SELECT e.last_name AS emp_name, e.salary AS emp_salary, 
       m.last_name AS mgr_name, m.salary AS mgr_salary 
FROM employee e, employee m 
WHERE e.manager_id = m.employee_id AND m.salary < e.salary
