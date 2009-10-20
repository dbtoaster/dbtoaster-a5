
LAUNCHER_SCRIPTS=node.sh client.sh unit.sh toaster.sh

all: $(LAUNCHER_SCRIPTS) dbtoaster.top

$(LAUNCHER_SCRIPTS) : %.sh : ../src/bin/launcher.in
	cat ../src/bin/launcher.in | sed s/--SCRIPT--/$*-launcher.rb/ > $@ 
	chmod +x $@

dbtoaster.top:
	make -C ../../../prototype/compiler/alpha3/ top $(TARGET)
	cp ../../../prototype/compiler/alpha3/dbtoaster.top ./

clean: 
	rm -f $(LAUNCHER_SCRIPTS)
