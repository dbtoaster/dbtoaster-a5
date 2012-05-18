CREATE STREAM R(A int, B int)
FROM FILE '../../experiments/data/tiny_r_deletions.dat' LINE DELIMITED
csv (fields := ',', triggers := '0:delete,1:insert');
