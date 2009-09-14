select sum(asks.p * asks.v), sum(asks.v)
	from asks
	where 0.25*(select sum(a2.v) from asks a2) > 
		(select sum(a1.v) from asks a1
			where a1.p > asks.p)