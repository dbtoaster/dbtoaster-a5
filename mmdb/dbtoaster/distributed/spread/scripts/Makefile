
LAUNCHER_SCRIPTS=node.sh client.sh unit.sh

all: $(LAUNCHER_SCRIPTS)

$(LAUNCHER_SCRIPTS) : %.sh : ../src/bin/launcher.in
	cat ../src/bin/launcher.in | sed s/--SCRIPT--/$*-launcher.rb/ > $@ 
	chmod +x $@

clean: 
	rm -f $(LAUNCHER_SCRIPTS)