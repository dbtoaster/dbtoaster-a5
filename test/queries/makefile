QUERIES := \
	simple/rst \
	tpch/query3 \
	tpch/query11 \
	tpch/ssb4 \
    finance/axfinder \
    finance/brokerspread	

SQL_QUERIES := $(patsubst %,%.sql,$(QUERIES))
M3_QUERIES  := $(patsubst %,%.m3,$(QUERIES))

all: m3queries

m3queries: $(M3_QUERIES)

clean:
	rm -f $(M3_QUERIES)

$(M3_QUERIES) : %.m3 : %.sql
	../../bin/dbtoaster -l m3 $< > $@

.PHONY: all m3queries clean
