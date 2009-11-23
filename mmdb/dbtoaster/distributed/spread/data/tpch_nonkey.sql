
create table customer(custkey int, nationkey int); 
create table orders(orderkey int, custkey int);
create table lineitem(orderkey int, suppkey int, partkey int, quantity int);
create table supplier(suppkey int, nationkey int);

select sum(l.quantity) from
    customer c, supplier s,
    orders o, lineitem l
where
    c.nationkey = s.nationkey
and s.suppkey = l.suppkey
and c.custkey = o.custkey
and l.orderkey = o.orderkey
group by l.partkey
--select sum(l.quantity)
--from lineitem l
--group by l.partkey