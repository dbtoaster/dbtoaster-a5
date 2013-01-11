BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE RESULTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;

/

CREATE TABLE RESULTS (
    trj_id           integer,
    t                integer,
    avg_length       float
);

SET SERVEROUTPUT ON;


-- Query 1: compute the radial distribution of all water molecules from a reference

CREATE OR REPLACE PROCEDURE recompute_query AS
BEGIN
    DELETE FROM RESULTS;

    INSERT INTO RESULTS (
		select P.trj_id, P.t, 
			   case when avg(vec_length(P.x-P2.x, P.y-P2.y, P.z-P2.z)) is null then 0
			        else avg(vec_length(P.x-P2.x, P.y-P2.y, P.z-P2.z)) end
		from AtomPositions P, AtomMeta M,
			 AtomPositions P2, AtomMeta M2
		where P.trj_id        = P2.trj_id 
		and   P.t             = P2.t
		and   P.atom_id       = M.atom_id
		and   P2.atom_id      = M2.atom_id
		and   M.residue_name  = 'LYS'
		and   M.atom_name     = 'NZ'
		and   M2.residue_name = 'TIP3'
		and   M2.atom_name    = 'OH2'
		group by P.trj_id, P.t
    );
END;

/

CREATE TRIGGER refresh_atompositions
  AFTER INSERT OR DELETE ON AtomPositions
BEGIN
  recompute_query(); 
END;

/

CREATE DIRECTORY q1log AS '/tmp';
CALL dispatch('Q1LOG', 'query1.log');
SELECT * FROM RESULTS;


DROP TABLE RESULTS;
DROP PROCEDURE recompute_query;
DROP DIRECTORY q1log;
DROP TRIGGER refresh_atompositions;

exit