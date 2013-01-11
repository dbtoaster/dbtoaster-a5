-- The main "streaming" table of molecular trajectories
-- which is populated by long-running MD simulations.
-- A table of raw trajectories, defining (x,y,z)- positions of 
-- the atoms comprising a protein.
CREATE TABLE AtomPositions (
    trj_id  integer,
    t       integer,
    atom_id integer,
    x       float,
    y       float,
    z       float
)   ;

-- Static tables
-- These will be preloaded prior to trajectory ingestion.

-- Chemical information about an atom.
CREATE TABLE AtomMeta (
    protein_id   integer,
    atom_id      integer,
    atom_type    varchar2(100),
    atom_name    varchar2(100),
    residue_id   integer,
    residue_name varchar2(100),
    segment_name varchar2(100)
)   ;

-- Protein structure information, as bonded atom pairs, triples and dihedrals
CREATE TABLE Bonds (
    protein_id   integer,
    atom_id1     integer,
    atom_id2     integer,
    bond_const   float,
    bond_length  float
)   ;

CREATE TABLE Angles (
    protein_id  integer,
    atom_id1    integer,
    atom_id2    integer,
    atom_id3    integer,
    angle_const float,
    angle       float
)   ;

CREATE TABLE Dihedrals (
    protein_id  integer,
    atom_id1    integer,
    atom_id2    integer,
    atom_id3    integer,
    atom_id4    integer,
    force_const float,
    n           float,
    delta       float
)   ;

CREATE TABLE ImproperDihedrals (
    protein_id  integer,
    atom_id1    integer,
    atom_id2    integer,
    atom_id3    integer,
    atom_id4    integer,
    force_const float,
    delta       float
)   ;

CREATE TABLE NonBonded (
    protein_id  integer,
    atom_id1    integer,
    atom_id2    integer,
    atom_ty1    varchar2(100),
    atom_ty2    varchar2(100),
    rmin        float,
    eps         float,
    acoef       float,
    bcoef       float,
    charge1     float,
    charge2     float
)   ;

-- A helper table to automatically generate unique ids for conformations
CREATE TABLE ConformationPoints (
  trj_id        integer,
  t             integer,
  point_id      integer
) ;

-- A helper table for conformation features, to ensure equivalence of
-- features over whole trajectories.
CREATE TABLE Dimensions (
    atom_id1    integer,
    atom_id2    integer,
    atom_id3    integer,
    atom_id4    integer,
    dim_id      integer
) ;
  
-- An n-dimensional histogram specification.
CREATE TABLE Buckets (
  dim_id          integer,
  bucket_id       integer,
  bucket_start    float,
  bucket_end      float  
) ;


CREATE TABLE Agenda (
  schema           character varying(25),
  event            integer,
  acoef            float,
  angle            float,
  angle_const      float,
  atom_id          integer,
  atom_id1         integer,
  atom_id2         integer,
  atom_id3         integer,
  atom_id4         integer,
  atom_name        varchar2(100),
  atom_ty1         varchar2(100),
  atom_ty2         varchar2(100),
  atom_type        varchar2(100),
  bcoef            float,
  bond_const       float,
  bond_length      float,
  bucket_end       float,
  bucket_id        integer,
  bucket_start     float,
  charge1          float,
  charge2          float,
  delta            float,
  dim_id           integer,
  eps              float,
  force_const      float,
  n                float,
  point_id         integer,
  protein_id       integer,
  residue_name     varchar2(100),
  rmin             float,
  segment_name     varchar2(100),
  t                integer,
  trj_id           integer,
  x                float,
  y                float,
  z                float 
);


SET SERVEROUTPUT ON;

CREATE OR REPLACE PROCEDURE dispatch(log_dir IN VARCHAR2, log_file_name IN VARCHAR2)
AS
    item AGENDA%ROWTYPE;

    tally integer;
    result_count integer;
    total_count integer;
    
    CURSOR agenda_iterator IS SELECT * FROM AGENDA;

    total_milis integer;
    start_ts   TIMESTAMP;
    ts         TIMESTAMP;
    log_file   UTL_FILE.FILE_TYPE;

BEGIN
    tally := 0;
    SELECT count(*) INTO total_count FROM AGENDA;
    SELECT SYSTIMESTAMP INTO start_ts FROM DUAL;
    
    log_file := UTL_FILE.FOPEN(log_dir, log_file_name, 'A');
                

    OPEN agenda_iterator;
    LOOP
        FETCH agenda_iterator INTO item;
        EXIT WHEN agenda_iterator%NOTFOUND;

		case item.schema

          when 'ATOMPOSITIONS'
          then case item.event

            when 1
            then INSERT INTO ATOMPOSITIONS values
              (item.trj_id, item.t, item.atom_id, item.x, item.y, item.z);
            
            when 0
            then DELETE FROM ATOMPOSITIONS 
            		where trj_id = item.trj_id 
            			  AND t = item.t 
            			  AND atom_id = item.atom_id;

          end case;

          when 'ATOMMETA'
          then case item.event

            when 1
            then INSERT INTO ATOMMETA values
              (item.protein_id, item.atom_id, item.atom_type, item.atom_name, item.residue_id,
               item.residue_name, item.segment_name);
            
          end case;

          when 'BONDS'
          then case item.event 

            when 1
            then INSERT INTO BONDS values 
              (item.protein_id, item.atom_id1, item.atom_id2, item.bond_const, item.bond_length);
            
          end case;

          when 'ANGLES'
          then case item.event

            when 1
            then INSERT INTO ANGLES values
              (item.protein_id, item.atom_id1, item.atom_id2, item.atom_id3, item.angle_const, 
               item.angle);
            
          end case;

          when 'DIHEDRALS'
          then case item.event

            when 1
            then INSERT INTO DIHEDRALS values
              (item.protein_id, item.atom_id1, item.atom_id2, item.atom_id3, item.atom_id4, 
               item.force_const, item.n, item.delta);
            
          end case;

          when 'IMPROPERDIHEDRALS'
          then case item.event

            when 1
            then INSERT INTO IMPROPERDIHEDRALS values 
              (item.protein_id, item.atom_id1, item.atom_id2, item.atom_id3, item.atom_id4, 
               item.force_const, item.delta);
            
          end case;

          when 'NONBONDED'
          then case item.event

            when 1
            then INSERT INTO NONBONDED values
              (item.protein_id, item.atom_id1, item.atom_id2, item.atom_ty1, item.atom_ty2,
               item.rmin, item.eps, item.acoef, item.bcoef, item.charge1, item.charge2);

          end case;

          when 'CONFORMATIONPOINTS'
          then case item.event

            when 1
            then INSERT INTO CONFORMATIONPOINTS values
              (item.trj_id, item.t, item.point_id);

          end case;
          
          when 'DIMENSIONS'
          then case item.event

            when 1
            then INSERT INTO DIMENSIONS values
              (item.atom_id1, item.atom_id2, item.atom_id3, item.atom_id4, item.dim_id);

          end case;
          
          when 'BUCKETS'
          then case item.event

            when 1
            then INSERT INTO BUCKETS values
              (item.dim_id, item.bucket_id, item.bucket_start, item.bucket_end);

          end case;
          
        end case;
        
        IF item.schema = 'ATOMPOSITIONS' THEN
			tally := tally + 1;
    	    EXECUTE IMMEDIATE 'SELECT count(*) FROM RESULTS' INTO result_count;
        
            SELECT SYSTIMESTAMP INTO ts FROM DUAL;
            SELECT (extract( day from diff )*24*60*60*1000 +
               extract( hour from diff )*60*60*1000 +
               extract( minute from diff )*60*1000 +
               round(extract( second from diff )*1000)) INTO total_milis
            FROM (SELECT ts - start_ts AS diff FROM dual);

            UTL_FILE.PUT_LINE(log_file,
               to_char(total_milis) || ',' ||
               to_char(tally) || ',' ||
               to_char(result_count));
        
            /*
               TO_CHAR (ts, 'MM-DD-YYYY HH24:MI:SS:FF') || ',' || 
            UTL_FILE.FFLUSH(log_file);
    	    */
    	    
    	    EXIT WHEN total_milis >= (@@TIMEOUT@@ * 1000);	        
    	END IF;
    END LOOP;

    CLOSE agenda_iterator;

	UTL_FILE.PUT_LINE(log_file, to_char(total_count));
    UTL_FILE.FCLOSE(log_file);
	
END dispatch;

/

SHOW ERRORS;


CREATE OR REPLACE FUNCTION vec_length(x in float, y in float, z in float) RETURN float
AS
BEGIN
  RETURN sqrt(x*x + y*y + z*z);
END;

/

CREATE OR REPLACE FUNCTION vec_dot(x1 in float, y1 in float, z1 in float,
                                   x2 in float, y2 in float, z2 in float) RETURN float
AS
BEGIN
  RETURN (x1*x2 + y1*y2 + z1*z2);
END;

/

CREATE OR REPLACE FUNCTION vector_angle(
                              x1 in float, y1 in float, z1 in float,
                              x2 in float, y2 in float, z2 in float) RETURN float
AS
BEGIN
  RETURN acos( vec_dot(x1,y1,z1,x2,y2,z2) / 
  				( vec_length(x1,y1,z1) * vec_length(x2,y2,z2) ) );
END;

/

CREATE OR REPLACE FUNCTION dihedral_angle(
                              x1 in float, y1 in float, z1 in float,
                              x2 in float, y2 in float, z2 in float,
                              x3 in float, y3 in float, z3 in float,
                              x4 in float, y4 in float, z4 in float) RETURN float
AS
  v1x float;
  v1y float;
  v1z float;
  v2x float;
  v2y float;
  v2z float;
  v3x float;
  v3y float;
  v3z float;

  n1x float;
  n1y float;
  n1z float;
  n2x float;
  n2y float;
  n2z float;
  
BEGIN
  v1x := x2 - x1;
  v1y := y2 - y1;
  v1z := z2 - z1;
  
  v2x := x3 - x2;
  v2y := y3 - y2;
  v2z := z3 - z2;
  
  v3x := x4 - x3;
  v3y := y4 - y3;
  v3z := z4 - z3;
  
  n1x := (v1y*v2z-v1z*v2y);
  n1y := (v1z*v2x-v1x*v2z);
  n1z := (v1x*v2y-v1y*v2x);

  n2x := (v2y*v3z-v2z*v3y);
  n2y := (v2z*v3x-v2x*v3z);
  n2z := (v2x*v3y-v2y*v3x);

  RETURN atan2( vec_length(v2x,v2y,v2z) * vec_dot(v1x,v1y,v1z,n2x,n2y,n2z), 
  				vec_dot(n1x,n1y,n1z,n2x,n2y,n2z) );
END;

/


CREATE OR REPLACE FUNCTION mddb_mod64(x in number) RETURN integer
AS
BEGIN
	return mod(x,18446744073709551616);
END;

/

CREATE OR REPLACE FUNCTION mddb_xor(x in integer, y in integer) RETURN integer
AS
BEGIN
	return mddb_mod64((x + y) - BitAND(x, y) * 2);
END;

/


CREATE OR REPLACE FUNCTION mddb_hash(n in integer) RETURN integer
AS
   v integer;
BEGIN
      v := mddb_mod64( mddb_mod64(n * 3935559000370003845) + 2691343689449507681);
      v := mddb_xor( v, floor(v / power(2,21))); 
      v := mddb_xor( v, mddb_mod64(v * power(2,37))); 
      v := mddb_xor( v, floor(v / 16));
      v := mddb_mod64( v * 4768777413237032717 );
      v := mddb_xor( v, floor(v / power(2,20))); 
      v := mddb_xor( v, mddb_mod64(v * power(2,41))); 
      v := mddb_xor( v, floor(v / 32));
      RETURN v;
END;

/


exit
