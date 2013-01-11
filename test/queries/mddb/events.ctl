load data
 infile '@@PATH@@/@@AGENDA@@'
 into table agenda
 fields terminated by "|"      
 ( schema, event,
   acoef, angle, angle_const, atom_id, atom_id1, atom_id2, atom_id3, atom_id4,
   atom_name, atom_ty1, atom_ty2, atom_type, bcoef, bond_const, bond_length,
   bucket_end, bucket_id, bucket_start, charge1, charge2, delta, dim_id,
   eps, force_const, n, point_id, protein_id, residue_name, rmin, segment_name,
   t, trj_id, x, y, z terminated by "\r" )