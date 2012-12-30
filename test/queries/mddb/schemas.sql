-- Simplified MDDB schema and example workload for benchmarking DBToaster.

-- The main "streaming" table of molecular trajectories
-- which is populated by long-running MD simulations.
-- A table of raw trajectories, defining (x,y,z)- positions of 
-- the atoms comprising a protein.
create stream AtomPositions (
    trj_id  int,
    t       int,
    atom_id int,
    x       float,
    y       float,
    z       float
)   FROM FILE '../../experiments/data/tpch/standard/lineitem.csv'
  LINE DELIMITED CSV (delimiter := '|');

-- Static tables
-- These will be preloaded prior to trajectory ingestion.

-- Chemical information about an atom.
create table AtomMeta (
    protein_id   int,
    atom_id      int,
    atom_type    varchar(100),
    atom_name    varchar(100),
    residue_id   int,
    residue_name varchar(100),
    segment_name varchar(100)
)   FROM FILE '../../experiments/data/tpch/standard/lineitem.csv'
  LINE DELIMITED CSV (delimiter := '|');

-- Protein structure information, as bonded atom pairs, triples and dihedrals
create table Bonds (
    protein_id   int,
    atom_id1     int,
    atom_id2     int,
    bond_const   float,
    bond_length  float
);

create table Angles (
    protein_id  int,
    atom_id1    int,
    atom_id2    int,
    atom_id3    int,
    angle_const float,
    angle       float
);

create table Dihedrals (
    protein_id  int,
    atom_id1    int,
    atom_id2    int,
    atom_id3    int,
    atom_id4    int,
    force_const float,
    n           float,
    delta       float
);

create table ImproperDihedrals (
    protein_id  int,
    atom_id1    int,
    atom_id2    int,
    atom_id3    int,
    atom_id4    int,
    force_const float,
    delta       float
);

create table NonBonded (
    protein_id  int,
    atom_id1    int,
    atom_id2    int,
    atom_ty1    varchar(100),
    atom_ty2    varchar(100),
    rmin        float,
    eps         float,
    acoef       float,
    bcoef       float,
    charge1     float,
    charge2     float
);

-- A helper table to automatically generate unique ids for conformations
create table ConformationPoints (
  trj_id        int,
  t             int,
  point_id      int
);

-- A helper table for conformation features, to ensure equivalence of
-- features over whole trajectories.
create table Dimensions (
    atom_id1    int,
    atom_id2    int,
    atom_id3    int,
    atom_id4    int,
    dim_id      int
);
---create index Dimensions_idIndex on Dimensions (dim_id);

-- An n-dimensional histogram specification.
create table Buckets (
  dim_id          int,
  bucket_id       int,
  bucket_start    float,
  bucket_end      float  
);


-- Utility functions.
