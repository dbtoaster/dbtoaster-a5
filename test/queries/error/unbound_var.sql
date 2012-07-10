CREATE STREAM r(a INT);

SELECT b.foo
FROM (select sum(a.notexists) as foo
from (select count(*) from r) a) b
