--  List all job details

CREATE STREAM JOB(
    job_id       INT,
    job_function VARCHAR(20)
    ) 
  FROM FILE '../dbtoaster-experiments-data/employee/job.dat' LINE DELIMITED
  CSV ();

SELECT * 
FROM job;
