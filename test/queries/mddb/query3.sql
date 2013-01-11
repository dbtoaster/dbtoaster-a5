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
    t      integer,
    total  float
);



SET SERVEROUTPUT ON;

--
-- Single unified version of query 3.
--

CREATE OR REPLACE PROCEDURE recompute_query AS
BEGIN
    DELETE FROM RESULTS;

    INSERT INTO RESULTS (
		select BondE.t,
			   case when sum(BondE.e + AngleE.e + DihedralE.e + 
			                 NonBondedE.vw_ij + NonBondedE.e_ij) is null then 0
			        else sum(BondE.e + AngleE.e + DihedralE.e + 
			                 NonBondedE.vw_ij + NonBondedE.e_ij) end as e
		from 
		( select P.t,
				 case when sum(0.5 * B.bond_const * 
				               power(vec_length(P.x-P2.x,P.y-P2.y,P.z-P2.z)-B.bond_length, 2)) is null then 0
				      else sum(0.5 * B.bond_const * 
				               power(vec_length(P.x-P2.x,P.y-P2.y,P.z-P2.z)-B.bond_length, 2)) end as e
		  from  Bonds B, AtomPositions P, AtomPositions P2
		  where B.atom_id1 = P.atom_id
		  and   B.atom_id2 = P2.atom_id
		  and   P.t = P2.t
		  group by P.t 
		) BondE,
		( select P.t,
				 case when
				 	sum(power(radians(1),2) * A.angle_const *
					 	power(degrees(vector_angle(P.x-P2.x,P.y-P2.y,P.z-P2.z,
									            P3.x-P2.x,P3.y-P2.y,P3.z-P2.z))
						 	  - A.angle, 2)) is null then 0
				 else
				    sum(power(radians(1),2) * A.angle_const *
					 	power(degrees(vector_angle(P.x-P2.x,P.y-P2.y,P.z-P2.z,
									            P3.x-P2.x,P3.y-P2.y,P3.z-P2.z))
						 	  - A.angle, 2)) end as e
		  from Angles A, AtomPositions P, AtomPositions P2, AtomPositions P3
		  where A.atom_id1 = P.atom_id
		  and   A.atom_id2 = P2.atom_id
		  and   A.atom_id3 = P3.atom_id
		  and   P.t = P2.t and P.t = P3.t
		  group by P.t 
		) AngleE,
		( select R.t, 
		         case when sum(R.force_const * (1 + cos(R.n * degrees(R.d_angle) - R.delta))) is null then 0
		              else sum(R.force_const * (1 + cos(R.n * degrees(R.d_angle) - R.delta))) end as e
		  from
			(select P.t, D.force_const, D.n, D.delta,
					dihedral_angle(P.x,P.y,P.z,
								   P2.x,P2.y,P2.z,
								   P3.x,P3.y,P3.z,
								   P4.x,P4.y,P4.z) as d_angle 
			 from Dihedrals D,
				  AtomPositions P, AtomPositions P2, AtomPositions P3, AtomPositions P4,
				  AtomMeta M,      AtomMeta M2,      AtomMeta M3,      AtomMeta M4
			 where ((D.atom_id1 = M.atom_id  or M.atom_type  = 'X') and M.atom_id  = P.atom_id)
			 and   ((D.atom_id2 = M2.atom_id or M2.atom_type = 'X') and M2.atom_id = P2.atom_id)
			 and   ((D.atom_id3 = M3.atom_id or M3.atom_type = 'X') and M3.atom_id = P3.atom_id)
			 and   ((D.atom_id4 = M4.atom_id or M4.atom_type = 'X') and M4.atom_id = P4.atom_id)
			 and   P.t = P2.t and P.t = P3.t and P.t = P4.t
			 ) R
		  group by t 
		) DihedralE,
		( select R.t, 
		         case when sum((R.force_const * power(radians(1),2)) * 
		                       power((1.0 * degrees(R.d_angle) - R.delta),2)) is null then 0
		              else sum((R.force_const * power(radians(1),2)) * 
		                       power((1.0 * degrees(R.d_angle) - R.delta),2)) end as e
		  from
			(select P.t, D.force_const, D.delta,
					dihedral_angle(P.x,P.y,P.z,
								   P2.x,P2.y,P2.z,
								   P3.x,P3.y,P3.z,
								   P4.x,P4.y,P4.z) as d_angle
			 from ImproperDihedrals D,
				  AtomPositions P, AtomPositions P2, AtomPositions P3, AtomPositions P4,
				  AtomMeta M,      AtomMeta M2,      AtomMeta M3,      AtomMeta M4
			 where ((D.atom_id1 = M.atom_id  or M.atom_type  = 'X') and M.atom_id  = P.atom_id)
			 and   ((D.atom_id2 = M2.atom_id or M2.atom_type = 'X') and M2.atom_id = P2.atom_id)
			 and   ((D.atom_id3 = M3.atom_id or M3.atom_type = 'X') and M3.atom_id = P3.atom_id)
			 and   ((D.atom_id4 = M4.atom_id or M4.atom_type = 'X') and M4.atom_id = P4.atom_id)
			 and   P.t = P2.t and P.t = P3.t and P.t = P4.t
			 ) as R
		  group by R.t
		) ImproperE,
		( select R.t, 
				 case when sum(R.acoef / power(R.r_ij, 12.0) - R.bcoef / power(R.r_ij, 6.0)) is null then 0
				 	  else sum(R.acoef / power(R.r_ij, 12.0) - R.bcoef / power(R.r_ij, 6.0)) end AS vw_ij,
			     case when sum(R.q_ij / R.r_ij) is null then 0
			          else sum(R.q_ij / R.r_ij) end AS e_ij
		  from
			(select P.t, NB.atom_id1 as atom_id1, NB.atom_id2 as atom_id2,
						 NB.acoef                                       as acoef,
						 NB.bcoef                                       as bcoef,
						 vec_length(p.x - p2.x, p.y - p2.y, p.z - p2.z) as r_ij,
						 332 * NB.charge1 * NB.charge2                  as q_ij
			 from NonBonded NB,
				  AtomPositions P, AtomPositions P2
			 where NB.atom_id1 = P.atom_id
			 and   NB.atom_id2 = P2.atom_id
			 and   P.atom_id <> P2.atom_id
			 and   P.t = P2.t
			 and   vec_length(P.x-P2.x,P.y-P2.y,P.z-P2.z) <= 12
		
			 -- Avoid non-bonded pairs that actually exist as a bond.
			 and (
				  not exists
					(select B.atom_id1 from Bonds B
					 where (B.atom_id1 = NB.atom_id1 and B.atom_id2 = NB.atom_id2)
						or (B.atom_id2 = NB.atom_id1 and B.atom_id1 = NB.atom_id2))
				 )
		
			 -- We don't need to check 1-2 or 2-3 pairs since these are already
			 -- checked above in the bonds.
			 and (
				  not exists
					(select A.atom_id1 from Angles A
					 where (A.atom_id1 = NB.atom_id1 and A.atom_id3 = NB.atom_id2)
						or (A.atom_id1 = NB.atom_id2 and A.atom_id3 = NB.atom_id1))
				 )
			 ) R
		  group by R.t
		) NonBondedE
		where BondE.t = AngleE.t
		and   BondE.t = DihedralE.t
		and   BondE.t = NonBondedE.t
		and   BondE.t = ImproperE.t
		group by BondE.t
	);
END;

/

CREATE TRIGGER refresh_atompositions
  AFTER INSERT OR DELETE ON AtomPositions
BEGIN
  recompute_query(); 
END;

/

CREATE DIRECTORY q3log AS '/tmp';
CALL dispatch('Q3LOG', 'query3.log');
SELECT * FROM RESULTS;


DROP TABLE RESULTS;
DROP PROCEDURE recompute_query;
DROP DIRECTORY q3log;
DROP TRIGGER refresh_atompositions;

exit