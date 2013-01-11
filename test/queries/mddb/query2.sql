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
    bucket_id      integer,
    cnt            integer
);

SET SERVEROUTPUT ON;


-- Query 2: a simple relational histogram implementation for
-- conformation-space population frequencies.
-- I have simplified computing a unique bucket id for each point
-- to a hash-based implementation that will be fine provided the
-- input data is relatively sparse, and thus will have few collisions
-- (rather than MDDB's array-based approach).

CREATE OR REPLACE PROCEDURE recompute_query AS
BEGIN
    DELETE FROM RESULTS;

    INSERT INTO RESULTS (
		select bucket_id, 
		       case when count(*) is null then 0
		            else count(*) end
		from 
		(
		  select S.point_id, 
		         case when sum(mddb_hash(B.bucket_id)) is null then 0
		              else sum(mddb_hash(B.bucket_id)) end as bucket_id 
		  from 
		  (
			select C.point_id, C.trj_id, C.t, N.dim_id, N.phi_psi 
			from
			(
			  select P1.trj_id, P1.t,
					 dihedral_angle(P1.x,P1.y,P1.z,
									P2.x,P2.y,P2.z,
									P3.x,P3.y,P3.z,
									P4.x,P4.y,P4.z)	 as phi_psi,
					 DM.dim_id
			  from Dihedrals D,         Dimensions DM,
				   AtomPositions P1,    AtomPositions P2,    AtomPositions P3,    AtomPositions P4,
				   AtomMeta M1,         AtomMeta M2,         AtomMeta M3,         AtomMeta M4
			  where P1.t = P2.t           and P1.t = P3.t           and P1.t = P4.t
			  and   P1.trj_id = P2.trj_id and P1.trj_id = P3.trj_id and P1.trj_id = P4.trj_id
			  and   (D.atom_id1 = M1.atom_id and M1.atom_id = P1.atom_id)
			  and   (D.atom_id2 = M2.atom_id and M2.atom_id = P2.atom_id)
			  and   (D.atom_id3 = M3.atom_id and M3.atom_id = P3.atom_id)
			  and   (D.atom_id4 = M4.atom_id and M4.atom_id = P4.atom_id)
			  and   (D.atom_id1 = DM.atom_id1 and D.atom_id2 = DM.atom_id2 and
					 D.atom_id3 = DM.atom_id3 and D.atom_id4 = DM.atom_id4)
			  and (   (M1.atom_name = 'N'  and M2.atom_name = 'CA' and M3.atom_name = 'C')
				   or (M2.atom_name = 'N'  and M3.atom_name = 'CA' and M4.atom_name = 'C') )
			) N, ConformationPoints C
			where N.trj_id = C.trj_id and N.t = C.t
		  ) S, Buckets B
		  where S.dim_id = B.dim_id
		  and   (S.phi_psi > B.bucket_start) and (S.phi_psi < B.bucket_end)
		  group by point_id
		) R
		group by R.bucket_id
    );
END;

/

CREATE TRIGGER refresh_atompositions
  AFTER INSERT OR DELETE ON AtomPositions
BEGIN
  recompute_query(); 
END;

/

CREATE DIRECTORY q2log AS '/tmp';
CALL dispatch('Q2LOG', 'query2.log');
SELECT * FROM RESULTS;


DROP TABLE RESULTS;
DROP PROCEDURE recompute_query;
DROP DIRECTORY q2log;
DROP TRIGGER refresh_atompositions;

exit