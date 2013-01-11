INCLUDE 'test/queries/mddb/schemas.sql';

select P1.trj_id, P1.t, count(*) AS Q2
from Dihedrals D,
   AtomPositions P1,    AtomPositions P2,    AtomPositions P3,    AtomPositions P4
where (D.atom_id1 = P1.atom_id)
and   (D.atom_id2 = P2.atom_id)
and   (D.atom_id3 = P3.atom_id)
and   (D.atom_id4 = P4.atom_id)
group by P1.trj_id, P1.t