INCLUDE 'test/queries/mddb/schemas.sql';

--
-- Single unified version of query 3.
--
select BondE.t,
       sum(BondE.e + AngleE.e + DihedralE.e
           + NonBondedE.vw_ij + NonBondedE.e_ij) as e
from 
( select P.t,
         sum(0.5 * B.bond_const *
             pow(vec_length(P.x-P2.x,P.y-P2.y,P.z-P2.z)-B.bond_length, 2)) as e
  from  Bonds B, AtomPositions P, AtomPositions P2
  where B.atom_id1 = P.atom_id
  and   B.atom_id2 = P2.atom_id
  and   P.t = P2.t
  group by P.t )
as BondE,
( select P.t,
         sum(pow(radians(1),2) * A.angle_const *
             pow(degrees(
                 vector_angle(P.x-P2.x,P.y-P2.y,P.z-P2.z,
                              P3.x-P2.x,P3.y-P2.y,P3.z-P2.z))
                 - A.angle, 2)) as e
  from Angles A, AtomPositions P, AtomPositions P2, AtomPositions P3
  where A.atom_id1 = P.atom_id
  and   A.atom_id2 = P2.atom_id
  and   A.atom_id3 = P3.atom_id
  and   P.t = P2.t and P.t = P3.t
  group by P.t )
as AngleE,
( select R.t, sum(R.force_const * (1 + cos(R.n * degrees(R.d_angle) - R.delta))) as e
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
     and   P.t = P2.t and P.t = P3.t and P.t = P4.t) as R
  group by t )
as DihedralE,
( select R.t, sum((R.force_const * pow(radians(1),2))
                * pow((1.0 * degrees(R.d_angle) - R.delta),2)) as e
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
     and   P.t = P2.t and P.t = P3.t and P.t = P4.t) as R
  group by R.t)
as ImproperE,
( select R.t, sum(R.acoef / pow(R.r_ij, 12.0)
                  - R.bcoef / pow(R.r_ij, 6.0)) AS vw_ij,
            sum(R.q_ij / R.r_ij) AS e_ij
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
     ) as R
  group by R.t
) as NonBondedE
where BondE.t = AngleE.t
and   BondE.t = DihedralE.t
and   BondE.t = NonBondedE.t
and   BondE.t = ImproperE.t
group by BondE.t;