--  List all the department details

CREATE STREAM DEPARTMENT(
    department_id   INT,
    name            VARCHAR(20),
    location_id     INT
    ) 
  FROM FILE '../dbtoaster-experiments-data/employee/department.dat' LINE DELIMITED
  CSV ();

SELECT * 
FROM department;
