--  List all the department details

CREATE STREAM DEPARTMENT(
    department_id   INT,
    name            VARCHAR(20),
    location_id     INT
    ) 
  FROM FILE '../../experiments/data/employee/department.dat' LINE DELIMITED
  csv (fields := ',', schema := 'int,string,int', eventtype := 'insert');

SELECT * 
FROM department;
