TABLE_FILES := nation.tbl\
               partsupp.tbl\
               supplier.tbl\
	       customer.tbl\
	       part.tbl\
               orders.tbl\
               lineitem.tbl\
               region.tbl
TABLE_TARGETS := $(patsubst %,tpch_data/%,$(TABLE_FILES))

all: tables install

tables: tpch $(TABLE_TARGETS)
	rm -f tpch

tpch_data/%.tbl: %.tbl
	@echo Converting $<;
	cat $< | sed 's/|$$//' >$@
	rm $<

tpch:
	make --directory tpch_gen
	cd tpch_gen && ./dbgen -vfF -s 1 && mv *.tbl ../
	touch tpch

install:
	cat tpch-setup.sql | sed "s:%localdir%:$(shell pwd):" | psql tpch
	@echo "------------------------------------------"
	@echo "The data has been loaded.  If you have not"
	@echo "already done so, install pip into the tpch"
	@echo "database."
	@echo
	@echo "cd ../";
	@echo "scripts/install.sh tpch"
	@echo
	@echo "------------------------------------------"

clean:
	make --directory tpch_gen clean
	rm -f $(TABLE_TARGETS);