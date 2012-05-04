--  List all job details

CREATE STREAM JOB(
    job_id      INT,
    function    VARCHAR(20)
    ) 
  FROM FILE '../../experiments/data/employee/job.dat' LINE DELIMITED
  csv (fields := ',', schema := 'int,string', eventtype := 'insert');

SELECT job_id, function 
FROM job;
