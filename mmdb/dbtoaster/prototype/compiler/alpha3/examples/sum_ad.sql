create table R (a int, b int);
create table S (b int, c int);
create table T (c int, d int);

select sum(a*d) from r,s,t where r.b=s.b and s.c=t.c;
