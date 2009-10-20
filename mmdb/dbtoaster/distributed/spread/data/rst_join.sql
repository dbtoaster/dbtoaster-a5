--test R(6,65)
--test S(6,62)
--test S(606, 2)
--test T(6,602)
create table r(a int, b int); 
create table s(b int, c int); 
create table t(c int, d int); 
select sum(a*d) from r,s,t where r.b=s.b and s.c=t.c;